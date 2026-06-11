#include "kv_cache_manager/manager/migration_manager.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <tuple>
#include <utility>

#include "kv_cache_manager/common/logger.h"
#include "kv_cache_manager/common/request_context.h"
#include "kv_cache_manager/common/string_util.h"
#include "kv_cache_manager/common/timestamp_util.h"
#include "kv_cache_manager/data_storage/data_storage_manager.h"
#include "kv_cache_manager/data_storage/data_storage_uri.h"
#include "kv_cache_manager/event/event_manager.h"
#include "kv_cache_manager/event/spec_events/migration_event.h"
#include "kv_cache_manager/manager/meta_searcher.h"
#include "kv_cache_manager/meta/cache_location.h"
#include "kv_cache_manager/meta/common.h"
#include "kv_cache_manager/meta/meta_indexer.h"
#include "kv_cache_manager/meta/meta_indexer_manager.h"

namespace kv_cache_manager {

namespace {
constexpr auto kMonitorIdleSleep = std::chrono::milliseconds(50);
constexpr auto kFutureWaitTime = std::chrono::microseconds(200);
constexpr std::size_t kMarkCleanupScanBatchSize = 256;
} // namespace

// Mark 持久化属性名（block 级 property）。带 inner 前缀避免与业务属性冲突。
const std::string MigrationManager::PROPERTY_TIERED_WRITE_TARGET = "__mig_tier_target__";
const std::string MigrationManager::PROPERTY_TIERED_WRITE_TS = "__mig_tier_ts__";

MigrationManager::MigrationManager(std::shared_ptr<SchedulePlanExecutor> schedule_plan_executor,
                                   std::shared_ptr<MetaIndexerManager> meta_indexer_manager,
                                   std::shared_ptr<DataStorageManager> data_storage_manager,
                                   int64_t mark_timeout_ms,
                                   int64_t mark_cleanup_interval_ms,
                                   std::shared_ptr<MetricsRegistry> metrics_registry,
                                   std::shared_ptr<EventManager> event_manager)
    : schedule_plan_executor_(std::move(schedule_plan_executor))
    , meta_indexer_manager_(std::move(meta_indexer_manager))
    , data_storage_manager_(std::move(data_storage_manager))
    , metrics_registry_(std::move(metrics_registry))
    , event_manager_(std::move(event_manager))
    , mark_timeout_ms_(mark_timeout_ms > 0 ? mark_timeout_ms : kDefaultMarkTimeoutMs)
    , mark_cleanup_interval_ms_(mark_cleanup_interval_ms > 0 ? mark_cleanup_interval_ms
                                                             : kDefaultMarkCleanupIntervalMs) {
    if (metrics_registry_ != nullptr) {
        metrics_enabled_ = true;
        m_tasks_submitted_total_ = metrics_registry_->GetCounter("migration.tasks_submitted_total");
        m_tasks_completed_success_ =
            metrics_registry_->GetCounter("migration.tasks_completed_total", {{"status", "success"}});
        m_tasks_completed_failed_ =
            metrics_registry_->GetCounter("migration.tasks_completed_total", {{"status", "failed"}});
        m_tasks_active_ = metrics_registry_->GetGauge("migration.tasks_active");
        m_copy_bytes_total_ = metrics_registry_->GetCounter("migration.copy_bytes_total");
        m_copy_duration_ms_ = metrics_registry_->GetGauge("migration.copy_duration_ms");
        m_marks_active_ = metrics_registry_->GetGauge("migration.marks_active");
        m_marks_consumed_total_ = metrics_registry_->GetCounter("migration.marks_consumed_total");
        m_marks_expired_total_ = metrics_registry_->GetCounter("migration.marks_expired_total");
    }
}

void MigrationManager::UpdateActiveTasksGauge() {
    if (metrics_enabled_) {
        m_tasks_active_ = static_cast<double>(active_tasks_.size());
    }
}

void MigrationManager::UpdateMarksActiveGauge() {
    if (metrics_enabled_) {
        // best-effort：持久化方案下无内存表，近似为 added - cleared（不计随 block 回收）。
        const int64_t added = static_cast<int64_t>(stat_marks_added_.load(std::memory_order_relaxed));
        const int64_t cleared = static_cast<int64_t>(stat_marks_cleared_.load(std::memory_order_relaxed));
        m_marks_active_ = static_cast<double>(added > cleared ? added - cleared : 0);
    }
}

MigrationManager::~MigrationManager() { Stop(); }

void MigrationManager::Start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) {
        return; // already running
    }
    monitor_thread_ = std::thread([this]() { MonitorLoop(); });
    KVCM_LOG_INFO("MigrationManager started, mark_timeout_ms=%ld mark_cleanup_interval_ms=%ld",
                  mark_timeout_ms_,
                  mark_cleanup_interval_ms_);
}

void MigrationManager::Stop() {
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false)) {
        return; // not running
    }
    pending_cv_.notify_all();
    if (monitor_thread_.joinable()) {
        monitor_thread_.join();
    }
    KVCM_LOG_INFO("MigrationManager stopped");
}

ErrorCode MigrationManager::PrepareCopyTask(const std::string &trace_id,
                                            const MigrationRequest &request,
                                            CopyTaskContext &out_ctx,
                                            std::vector<DataStorageUri> &out_src_uris,
                                            std::vector<DataStorageUri> &out_dst_uris) {
    if (request.instance_id.empty() || request.src_location_id.empty() || request.src_storage_name.empty() ||
        request.dst_storage_name.empty()) {
        KVCM_LOG_WARN("[%s] migration bad args: instance_id/src_location_id/src_storage/dst_storage required",
                      trace_id.c_str());
        return EC_BADARGS;
    }
    if (!data_storage_manager_) {
        return EC_ERROR;
    }
    auto indexer = meta_indexer_manager_ ? meta_indexer_manager_->GetMetaIndexer(request.instance_id) : nullptr;
    if (!indexer) {
        KVCM_LOG_WARN("[%s] MetaIndexer not found for instance %s", trace_id.c_str(), request.instance_id.c_str());
        return EC_INSTANCE_NOT_EXIST;
    }

    // 1. 读取源 location。
    MetaSearcher meta_searcher(indexer);
    auto ctx = std::make_shared<RequestContext>(trace_id.empty() ? "migration_prepare" : trace_id);
    std::vector<CacheLocationMap> location_maps;
    BlockMask empty_mask;
    ErrorCode ec = meta_searcher.BatchGetLocation(ctx.get(), {request.block_key}, empty_mask, location_maps);
    if (ec != EC_OK || location_maps.empty()) {
        KVCM_LOG_WARN("[%s] BatchGetLocation failed for block_key %ld, ec %d",
                      trace_id.c_str(),
                      request.block_key,
                      ec);
        return EC_NOENT;
    }
    auto src_iter = location_maps[0].find(request.src_location_id);
    if (src_iter == location_maps[0].end()) {
        KVCM_LOG_WARN("[%s] source location %s not found for block_key %ld",
                      trace_id.c_str(),
                      request.src_location_id.c_str(),
                      request.block_key);
        return EC_NOENT;
    }
    if (src_iter->second == nullptr) {
        return EC_ERROR;
    }
    const CacheLocation &src_location = *src_iter->second;
    if (src_location.status() != CLS_SERVING) {
        KVCM_LOG_WARN("[%s] source location %s not SERVING (status %d), skip migration",
                      trace_id.c_str(),
                      request.src_location_id.c_str(),
                      src_location.status());
        return EC_MISMATCH;
    }
    if (src_location.location_specs().empty()) {
        KVCM_LOG_WARN("[%s] source location %s has no specs", trace_id.c_str(), request.src_location_id.c_str());
        return EC_ERROR;
    }

    // 2. 目标 storage 类型。
    auto dst_backend = data_storage_manager_->GetDataStorageBackend(request.dst_storage_name);
    if (!dst_backend) {
        KVCM_LOG_WARN("[%s] target storage %s not found", trace_id.c_str(), request.dst_storage_name.c_str());
        return EC_NOENT;
    }
    const DataStorageType dst_type = dst_backend->GetType();

    // 3. 逐 spec 在目标 storage 预分配空间，构建目标 location specs + src/dst uri 对。
    std::vector<LocationSpec> dst_specs;
    dst_specs.reserve(src_location.location_specs().size());
    out_src_uris.clear();
    out_dst_uris.clear();
    std::vector<DataStorageUri> allocated_for_rollback;
    std::uint64_t total_bytes = 0;

    auto rollback = [&]() {
        if (!allocated_for_rollback.empty()) {
            data_storage_manager_->Delete(ctx.get(), request.dst_storage_name, allocated_for_rollback, nullptr);
        }
    };

    for (const auto &src_spec : src_location.location_specs()) {
        DataStorageUri src_uri(src_spec.uri());
        if (!src_uri.Valid()) {
            KVCM_LOG_WARN("[%s] invalid source uri for spec %s", trace_id.c_str(), src_spec.name().c_str());
            rollback();
            return EC_ERROR;
        }
        std::uint64_t spec_size = 0;
        src_uri.GetParamAs<std::uint64_t>("size", spec_size);
        total_bytes += spec_size;

        std::string dst_key =
            request.instance_id + "/" + src_spec.name() + "/" + StringUtil::Uint64ToHex(request.block_key);
        auto create_results = data_storage_manager_->Create(
            ctx.get(), request.dst_storage_name, {dst_key}, static_cast<size_t>(spec_size), nullptr);
        if (create_results.size() != 1 || create_results[0].first != EC_OK) {
            KVCM_LOG_WARN("[%s] allocate dst space failed on %s for spec %s",
                          trace_id.c_str(),
                          request.dst_storage_name.c_str(),
                          src_spec.name().c_str());
            rollback();
            return EC_ERROR;
        }
        DataStorageUri dst_uri = create_results[0].second;
        allocated_for_rollback.push_back(dst_uri);
        out_src_uris.push_back(src_uri);
        out_dst_uris.push_back(dst_uri);
        dst_specs.emplace_back(src_spec.name(), dst_uri.ToUriString());
    }

    // 4. 建目标 location（BatchAddLocation 总是写 CLS_WRITING 并生成随机 location_id）。
    auto dst_location = std::make_shared<CacheLocation>();
    dst_location->set_type(dst_type);
    dst_location->set_spec_size(dst_specs.size());
    for (auto &spec : dst_specs) {
        dst_location->push_location_spec(std::move(spec));
    }
    std::vector<std::string> out_location_ids;
    ec = meta_searcher.BatchAddLocation(ctx.get(), {request.block_key}, {dst_location}, out_location_ids);
    if (ec != EC_OK || out_location_ids.empty() || out_location_ids[0].empty()) {
        KVCM_LOG_WARN("[%s] BatchAddLocation failed for block_key %ld, ec %d",
                      trace_id.c_str(),
                      request.block_key,
                      ec);
        rollback();
        return EC_ERROR;
    }

    out_ctx.instance_id = request.instance_id;
    out_ctx.block_key = request.block_key;
    out_ctx.src_location_id = request.src_location_id;
    out_ctx.src_storage_name = request.src_storage_name;
    out_ctx.dst_storage_name = request.dst_storage_name;
    out_ctx.dst_location_id = out_location_ids[0];
    out_ctx.retention = request.retention;
    out_ctx.total_bytes = total_bytes;
    return EC_OK;
}

ErrorCode MigrationManager::Submit(const std::string &trace_id, MigrationRequest request) {
    // 同一 block 已有活跃任务则拒绝（防重复迁移）。
    {
        std::lock_guard<std::mutex> lock(task_mutex_);
        if (active_tasks_.count(request.block_key) > 0) {
            KVCM_LOG_INFO("[%s] block_key %ld already has an active migration task, skip",
                          trace_id.c_str(),
                          request.block_key);
            return EC_EXIST;
        }
    }

    CopyTaskContext ctx;
    std::vector<DataStorageUri> src_uris;
    std::vector<DataStorageUri> dst_uris;
    ErrorCode prepare_ec = PrepareCopyTask(trace_id, request, ctx, src_uris, dst_uris);
    if (prepare_ec != EC_OK) {
        return prepare_ec;
    }
    ctx.submit_time = std::chrono::steady_clock::now();

    // 登记活跃任务（用于防重复迁移和 copy 并发预算统计）。
    {
        std::lock_guard<std::mutex> lock(task_mutex_);
        if (active_tasks_.count(request.block_key) > 0) {
            // 并发竞争：已被其它请求占用，回滚刚建好的目标 location。
            SubmitTargetLocationDelete(ctx);
            return EC_EXIST;
        }
        active_tasks_.emplace(request.block_key, ctx);
        UpdateActiveTasksGauge();
    }

    // 提交 copy 任务。
    CacheLocationCopyRequest copy_req;
    copy_req.instance_id = ctx.instance_id;
    copy_req.block_key = ctx.block_key;
    copy_req.exec_storage_name = ctx.src_storage_name; // 一期 = 源 storage
    copy_req.src_uris = std::move(src_uris);
    copy_req.dst_uris = std::move(dst_uris);

    std::future<PlanExecuteResult> future = schedule_plan_executor_->Submit(copy_req);
    if (!future.valid()) {
        KVCM_LOG_WARN("[%s] submit copy task failed for block_key %ld", trace_id.c_str(), ctx.block_key);
        SubmitTargetLocationDelete(ctx);
        std::lock_guard<std::mutex> lock(task_mutex_);
        active_tasks_.erase(ctx.block_key);
        UpdateActiveTasksGauge();
        return EC_ERROR;
    }

    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_copies_.push_back(PendingCopy{ctx.block_key, std::move(future)});
    }
    pending_cv_.notify_one();

    stat_copy_submitted_.fetch_add(1, std::memory_order_relaxed);
    if (metrics_enabled_) {
        ++m_tasks_submitted_total_;
    }
    if (event_manager_ != nullptr) {
        auto ev = std::make_shared<MigrationSubmittedEvent>(ctx.instance_id);
        ev->SetEventTriggerTime();
        ev->SetAdditionalArgs(ctx.block_key, ctx.src_storage_name, ctx.dst_storage_name, trace_id);
        event_manager_->Publish(ev);
    }
    KVCM_LOG_INFO("[%s] migration copy submitted: instance %s block_key %ld src_loc %s -> dst_storage %s (dst_loc %s)",
                  trace_id.c_str(),
                  ctx.instance_id.c_str(),
                  ctx.block_key,
                  ctx.src_location_id.c_str(),
                  ctx.dst_storage_name.c_str(),
                  ctx.dst_location_id.c_str());
    return EC_OK;
}

std::vector<ErrorCode> MigrationManager::BatchSubmit(const std::string &trace_id,
                                                     std::vector<MigrationRequest> requests) {
    std::vector<ErrorCode> results;
    results.reserve(requests.size());
    for (auto &req : requests) {
        results.push_back(Submit(trace_id, std::move(req)));
    }
    return results;
}

bool MigrationManager::IsSourceLocationServing(const CopyTaskContext &ctx) const {
    auto indexer = meta_indexer_manager_ ? meta_indexer_manager_->GetMetaIndexer(ctx.instance_id) : nullptr;
    if (!indexer) {
        return false;
    }

    MetaSearcher meta_searcher(indexer);
    auto rc = std::make_shared<RequestContext>("migration_check_source");
    std::vector<CacheLocationMap> location_maps;
    BlockMask empty_mask;
    ErrorCode ec = meta_searcher.BatchGetLocation(rc.get(), {ctx.block_key}, empty_mask, location_maps);
    if (ec != EC_OK || location_maps.empty()) {
        return false;
    }
    auto iter = location_maps[0].find(ctx.src_location_id);
    return iter != location_maps[0].end() && iter->second != nullptr && iter->second->status() == CLS_SERVING;
}

void MigrationManager::CompleteCopyTaskAsFailed(const CopyTaskContext &ctx, const std::string &fail_reason) {
    SubmitTargetLocationDelete(ctx);
    ClearTieredWriteMark(ctx.instance_id, ctx.block_key);
    {
        std::lock_guard<std::mutex> lock(task_mutex_);
        active_tasks_.erase(ctx.block_key);
        UpdateActiveTasksGauge();
    }
    stat_copy_failed_.fetch_add(1, std::memory_order_relaxed);
    if (metrics_enabled_) {
        ++m_tasks_completed_failed_;
    }
    if (event_manager_ != nullptr) {
        auto ev = std::make_shared<MigrationCompletedEvent>(ctx.instance_id);
        ev->SetEventTriggerTime();
        ev->SetAdditionalArgs(
            ctx.block_key, ctx.src_storage_name, ctx.dst_storage_name, 0, ctx.total_bytes, false, fail_reason);
        event_manager_->Publish(ev);
    }
}

void MigrationManager::OnTaskSuccess(int64_t block_key) {
    CopyTaskContext ctx;
    {
        std::lock_guard<std::mutex> lock(task_mutex_);
        auto iter = active_tasks_.find(block_key);
        if (iter == active_tasks_.end()) {
            return; // 已被取消或处理过
        }
        ctx = iter->second;
    }

    if (!IsSourceLocationServing(ctx)) {
        KVCM_LOG_WARN("migration source lost, block_key %ld src_loc %s, discard dst_loc %s",
                      block_key,
                      ctx.src_location_id.c_str(),
                      ctx.dst_location_id.c_str());
        CompleteCopyTaskAsFailed(ctx, "source_lost");
        return;
    }

    // CAS 目标 location WRITING -> SERVING。
    auto indexer = meta_indexer_manager_ ? meta_indexer_manager_->GetMetaIndexer(ctx.instance_id) : nullptr;
    bool promoted = false;
    if (indexer) {
        MetaSearcher meta_searcher(indexer);
        auto rc = std::make_shared<RequestContext>("migration_on_success");
        std::vector<std::vector<MetaSearcher::LocationCASTask>> cas_tasks{
            {MetaSearcher::LocationCASTask{ctx.dst_location_id, CLS_WRITING, CLS_SERVING}}};
        std::vector<std::vector<ErrorCode>> cas_results;
        ErrorCode ec = meta_searcher.BatchCASLocationStatus(rc.get(), {block_key}, cas_tasks, cas_results);
        promoted = (ec == EC_OK && !cas_results.empty() && !cas_results[0].empty() && cas_results[0][0] == EC_OK);
    }

    if (!promoted) {
        // 目标提升失败：清理目标半成品，源端保持不动（数据未受损），按失败收尾。
        KVCM_LOG_WARN("migration promote dst location failed, block_key %ld dst_loc %s, treat as failed",
                      block_key,
                      ctx.dst_location_id.c_str());
        CompleteCopyTaskAsFailed(ctx, "promote_failed");
        return;
    }

    // 目标已可用，先清除可能存在的 Mark，避免后续 StartWriteCache 重复写冷层。
    ClearTieredWriteMark(ctx.instance_id, block_key);

    // 按 retention 处理源端。
    if (ctx.retention == MigrationRetention::MIGRATION_RETENTION_DELETE_SOURCE) {
        SubmitSourceLocationDelete(ctx);
    }

    // 最后移除活跃任务。
    {
        std::lock_guard<std::mutex> lock(task_mutex_);
        active_tasks_.erase(block_key);
        UpdateActiveTasksGauge();
    }
    stat_copy_completed_.fetch_add(1, std::memory_order_relaxed);
    const int64_t duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::steady_clock::now() - ctx.submit_time)
                                    .count();
    if (metrics_enabled_) {
        ++m_tasks_completed_success_;
        m_copy_bytes_total_ += ctx.total_bytes;
        m_copy_duration_ms_ = static_cast<double>(duration_ms);
    }
    if (event_manager_ != nullptr) {
        auto ev = std::make_shared<MigrationCompletedEvent>(ctx.instance_id);
        ev->SetEventTriggerTime();
        ev->SetAdditionalArgs(
            block_key, ctx.src_storage_name, ctx.dst_storage_name, duration_ms, ctx.total_bytes, true, "");
        event_manager_->Publish(ev);
    }
    KVCM_LOG_INFO("migration completed: instance %s block_key %ld dst_loc %s retention %d",
                  ctx.instance_id.c_str(),
                  block_key,
                  ctx.dst_location_id.c_str(),
                  static_cast<int>(ctx.retention));
}

void MigrationManager::OnTaskFailed(int64_t block_key, ErrorCode reason) {
    CopyTaskContext ctx;
    {
        std::lock_guard<std::mutex> lock(task_mutex_);
        auto iter = active_tasks_.find(block_key);
        if (iter == active_tasks_.end()) {
            return;
        }
        ctx = iter->second;
    }

    // 失败：CAS 目标 WRITING -> DELETING 并删除目标半成品，源端不动。
    const std::string fail_reason = "copy_failed:" + std::to_string(static_cast<int>(reason));
    CompleteCopyTaskAsFailed(ctx, fail_reason);
    KVCM_LOG_WARN("migration failed: instance %s block_key %ld dst_loc %s reason %d",
                  ctx.instance_id.c_str(),
                  block_key,
                  ctx.dst_location_id.c_str(),
                  reason);
}

void MigrationManager::SubmitTargetLocationDelete(const CopyTaskContext &ctx) {
    if (!schedule_plan_executor_ || ctx.dst_location_id.empty()) {
        return;
    }
    // CacheLocationDelRequest 会把目标 location CAS 到 DELETING 后删存储/删元数据，清理半成品。
    CacheLocationDelRequest del_req;
    del_req.instance_id = ctx.instance_id;
    del_req.block_keys = {ctx.block_key};
    del_req.location_ids = {{ctx.dst_location_id}};
    schedule_plan_executor_->SubmitNonBlocking(del_req);
}

void MigrationManager::SubmitSourceLocationDelete(const CopyTaskContext &ctx) {
    if (!schedule_plan_executor_ || ctx.src_location_id.empty()) {
        return;
    }
    CacheLocationDelRequest del_req;
    del_req.instance_id = ctx.instance_id;
    del_req.block_keys = {ctx.block_key};
    del_req.location_ids = {{ctx.src_location_id}};
    schedule_plan_executor_->SubmitNonBlocking(del_req);
}

void MigrationManager::MonitorLoop() {
    auto next_mark_cleanup_time =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(mark_cleanup_interval_ms_);
    while (running_.load(std::memory_order_relaxed)) {
        const auto now = std::chrono::steady_clock::now();
        if (now >= next_mark_cleanup_time) {
            CleanupExpiredMarks();
            next_mark_cleanup_time =
                std::chrono::steady_clock::now() + std::chrono::milliseconds(mark_cleanup_interval_ms_);
            continue;
        }

        PendingCopy cell;
        bool has_cell = false;
        {
            std::unique_lock<std::mutex> lock(pending_mutex_);
            if (pending_copies_.empty()) {
                auto wait_duration = std::min(kMonitorIdleSleep,
                                              std::chrono::duration_cast<std::chrono::milliseconds>(
                                                  next_mark_cleanup_time - std::chrono::steady_clock::now()));
                if (wait_duration < std::chrono::milliseconds::zero()) {
                    wait_duration = std::chrono::milliseconds::zero();
                }
                pending_cv_.wait_for(lock, wait_duration, [this]() {
                    return !running_.load(std::memory_order_relaxed) || !pending_copies_.empty();
                });
            }
            if (!pending_copies_.empty()) {
                cell = std::move(pending_copies_.front());
                pending_copies_.pop_front();
                has_cell = true;
            }
        }
        if (!has_cell) {
            continue;
        }

        auto status = cell.future.wait_for(kFutureWaitTime);
        if (status != std::future_status::ready) {
            // 尚未完成，放回队尾稍后再查。
            std::lock_guard<std::mutex> lock(pending_mutex_);
            pending_copies_.push_back(std::move(cell));
            continue;
        }

        PlanExecuteResult result = cell.future.get();
        if (result.status == EC_OK) {
            OnTaskSuccess(cell.block_key);
        } else {
            OnTaskFailed(cell.block_key, result.status);
        }
    }

    // 退出时排空剩余 future（不再驱动状态流转，仅释放）。
    std::lock_guard<std::mutex> lock(pending_mutex_);
    pending_copies_.clear();
}

void MigrationManager::CleanupExpiredMarks() {
    if (meta_indexer_manager_ == nullptr) {
        return;
    }
    const auto indexers = meta_indexer_manager_->GetIndexers();
    for (const auto &[instance_id, indexer] : indexers) {
        if (!running_.load(std::memory_order_relaxed)) {
            return;
        }
        CleanupExpiredMarksForIndexer(instance_id, indexer);
    }
}

void MigrationManager::CleanupExpiredMarksForIndexer(const std::string &instance_id,
                                                     const std::shared_ptr<MetaIndexer> &indexer) {
    if (indexer == nullptr) {
        return;
    }

    std::string cursor = SCAN_BASE_CURSOR;
    do {
        std::string next_cursor;
        KeyVector keys;
        RequestContext scan_rc("migration_mark_cleanup_scan");
        const auto scan_ec = indexer->Scan(&scan_rc, cursor, kMarkCleanupScanBatchSize, next_cursor, keys);
        if (scan_ec != EC_OK) {
            KVCM_LOG_WARN("migration mark cleanup scan failed, instance %s, ec %d", instance_id.c_str(), scan_ec);
            return;
        }

        if (!keys.empty()) {
            PropertyMapVector props;
            RequestContext prop_rc("migration_mark_cleanup_props");
            indexer->GetProperties(&prop_rc, keys, {PROPERTY_TIERED_WRITE_TARGET, PROPERTY_TIERED_WRITE_TS}, props);
            const int64_t now_ms = TimestampUtil::GetCurrentTimeMs();
            for (size_t i = 0; i < props.size() && i < keys.size(); ++i) {
                const auto target_it = props[i].find(PROPERTY_TIERED_WRITE_TARGET);
                if (target_it == props[i].end() || target_it->second.empty()) {
                    continue;
                }
                const auto ts_it = props[i].find(PROPERTY_TIERED_WRITE_TS);
                const int64_t ts_ms =
                    (ts_it != props[i].end() && !ts_it->second.empty()) ? std::atoll(ts_it->second.c_str()) : 0;
                if (!IsMarkExpired(ts_ms)) {
                    continue;
                }
                ClearTieredWriteMarkInternal(instance_id, keys[i], true, target_it->second, now_ms - ts_ms);
            }
        }

        cursor = next_cursor;
    } while (running_.load(std::memory_order_relaxed) && cursor != SCAN_BASE_CURSOR);
}

std::shared_ptr<MetaIndexer> MigrationManager::GetIndexer(const std::string &instance_id) const {
    return meta_indexer_manager_ ? meta_indexer_manager_->GetMetaIndexer(instance_id) : nullptr;
}

bool MigrationManager::IsMarkExpired(int64_t mark_ts_ms) const {
    if (mark_ts_ms <= 0) {
        return false; // 无时间戳信息，保守视为未过期
    }
    return TimestampUtil::GetCurrentTimeMs() - mark_ts_ms > mark_timeout_ms_;
}

ErrorCode MigrationManager::MarkForTieredWrite(const std::string &instance_id,
                                               const std::vector<int64_t> &block_keys,
                                               const std::string &dst_storage_name) {
    if (dst_storage_name.empty() || block_keys.empty()) {
        return EC_OK;
    }
    auto indexer = GetIndexer(instance_id);
    if (indexer == nullptr) {
        KVCM_LOG_WARN("MarkForTieredWrite: meta indexer not found for instance %s", instance_id.c_str());
        return EC_INSTANCE_NOT_EXIST;
    }
    const std::string ts_str = std::to_string(TimestampUtil::GetCurrentTimeMs());
    // RMW：只写 property（out_new_locations 留空，不动 location）。不存在的 block 跳过（不给空 block 打标）。
    auto modifier = [&dst_storage_name, &ts_str](const LocationIdVector & /*existing*/,
                                                 ErrorCode get_ec,
                                                 size_t /*idx*/,
                                                 PropertyMap &upsert_property_map,
                                                 CacheLocationMap & /*out_new*/) -> ModifierResult {
        if (get_ec == EC_NOENT) {
            return {MA_SKIP, EC_OK};
        }
        if (get_ec != EC_OK) {
            return {MA_FAIL, get_ec};
        }
        upsert_property_map[PROPERTY_TIERED_WRITE_TARGET] = dst_storage_name;
        upsert_property_map[PROPERTY_TIERED_WRITE_TS] = ts_str;
        return {MA_OK, EC_OK};
    };
    RequestContext rc("migration_mark");
    KeyVector keys(block_keys.begin(), block_keys.end());
    auto result = indexer->ReadModifyWriteBlock(&rc, keys, modifier);
    stat_marks_added_.fetch_add(block_keys.size(), std::memory_order_relaxed);
    UpdateMarksActiveGauge();
    if (event_manager_ != nullptr) {
        for (int64_t block_key : block_keys) {
            auto ev = std::make_shared<MigrationMarkAddEvent>(instance_id);
            ev->SetEventTriggerTime();
            ev->SetAdditionalArgs(block_key, dst_storage_name);
            event_manager_->Publish(ev);
        }
    }
    return result.ec;
}

bool MigrationManager::IsMarkedForTieredWrite(const std::string &instance_id, int64_t block_key) const {
    std::string target;
    return ShouldWriteToTieredStorageByMark(instance_id, block_key, target);
}

std::string MigrationManager::GetTieredWriteTarget(const std::string &instance_id, int64_t block_key) const {
    std::string target;
    return ShouldWriteToTieredStorageByMark(instance_id, block_key, target) ? target : std::string();
}

bool MigrationManager::ClearTieredWriteMarkInternal(const std::string &instance_id,
                                                   int64_t block_key,
                                                   bool expired,
                                                   const std::string &dst_storage_name,
                                                   int64_t mark_age_ms) {
    auto indexer = GetIndexer(instance_id);
    if (indexer == nullptr) {
        return false;
    }
    std::string event_dst_storage = dst_storage_name;
    int64_t event_mark_age_ms = mark_age_ms;
    bool had_mark = expired;
    if (!expired) {
        RequestContext prop_rc("migration_clear_mark_props");
        PropertyMapVector props;
        indexer->GetProperties(&prop_rc, {block_key}, {PROPERTY_TIERED_WRITE_TARGET, PROPERTY_TIERED_WRITE_TS}, props);
        if (!props.empty()) {
            const auto target_it = props[0].find(PROPERTY_TIERED_WRITE_TARGET);
            if (target_it != props[0].end() && !target_it->second.empty()) {
                had_mark = true;
                event_dst_storage = target_it->second;
            }
            const auto ts_it = props[0].find(PROPERTY_TIERED_WRITE_TS);
            int64_t mark_ts_ms = 0;
            if (ts_it != props[0].end() && !ts_it->second.empty() &&
                StringUtil::StrToInt64(ts_it->second.c_str(), mark_ts_ms) && mark_ts_ms > 0) {
                event_mark_age_ms = std::max<int64_t>(0, TimestampUtil::GetCurrentTimeMs() - mark_ts_ms);
            }
        }
    }
    bool cleared = false;
    // RMW：把 mark 属性置空作墓碑（读侧把空视为未打标）。block 不存在则 no-op（幂等）。
    auto modifier = [&cleared](const LocationIdVector & /*existing*/,
                               ErrorCode get_ec,
                               size_t /*idx*/,
                               PropertyMap &upsert_property_map,
                               CacheLocationMap & /*out_new*/) -> ModifierResult {
        if (get_ec != EC_OK) {
            return {MA_SKIP, EC_OK};
        }
        upsert_property_map[PROPERTY_TIERED_WRITE_TARGET] = "";
        upsert_property_map[PROPERTY_TIERED_WRITE_TS] = "";
        cleared = true;
        return {MA_OK, EC_OK};
    };
    RequestContext rc("migration_clear_mark");
    indexer->ReadModifyWriteBlock(&rc, {block_key}, modifier);
    if (cleared) {
        if (had_mark) {
            stat_marks_cleared_.fetch_add(1, std::memory_order_relaxed);
            if (expired) {
                stat_marks_expired_.fetch_add(1, std::memory_order_relaxed);
            }
            UpdateMarksActiveGauge();
            if (metrics_enabled_) {
                if (expired) {
                    ++m_marks_expired_total_;
                } else {
                    ++m_marks_consumed_total_;
                }
            }
        }
        if (event_manager_ != nullptr && had_mark) {
            if (expired) {
                auto ev = std::make_shared<MigrationMarkExpiredEvent>(instance_id);
                ev->SetEventTriggerTime();
                ev->SetAdditionalArgs(block_key, event_dst_storage, event_mark_age_ms);
                event_manager_->Publish(ev);
            } else {
                auto ev = std::make_shared<MigrationMarkConsumedEvent>(instance_id);
                ev->SetEventTriggerTime();
                ev->SetAdditionalArgs(block_key, event_dst_storage, event_mark_age_ms);
                event_manager_->Publish(ev);
            }
        }
    }
    return cleared;
}

void MigrationManager::ClearTieredWriteMark(const std::string &instance_id, int64_t block_key) {
    static_cast<void>(ClearTieredWriteMarkInternal(instance_id, block_key, false));
}

bool MigrationManager::ShouldWriteToTieredStorage(const std::string &instance_id,
                                                  int64_t block_key,
                                                  std::string &target) const {
    // 统一入口：先检查显式 Mark，后续可在这里串接更多 tiered write 策略。
    if (ShouldWriteToTieredStorageByMark(instance_id, block_key, target)) {
        return true;
    }
    return false;
}

bool MigrationManager::ShouldWriteToTieredStorageByMark(const std::string &instance_id,
                                                        int64_t block_key,
                                                        std::string &target) const {
    auto indexer = GetIndexer(instance_id);
    if (indexer == nullptr) {
        return false;
    }
    RequestContext rc("migration_query_mark");
    PropertyMapVector props;
    indexer->GetProperties(&rc, {block_key}, {PROPERTY_TIERED_WRITE_TARGET, PROPERTY_TIERED_WRITE_TS}, props);
    if (props.empty()) {
        return false;
    }
    auto tit = props[0].find(PROPERTY_TIERED_WRITE_TARGET);
    if (tit == props[0].end() || tit->second.empty()) {
        return false; // 未打标 / 已清（墓碑）
    }
    auto tsit = props[0].find(PROPERTY_TIERED_WRITE_TS);
    const int64_t ts_ms = (tsit != props[0].end() && !tsit->second.empty()) ? std::atoll(tsit->second.c_str()) : 0;
    if (IsMarkExpired(ts_ms)) {
        return false; // 惰性过期：超时视为未打标
    }
    target = tit->second;
    return true;
}

ErrorCode MigrationManager::BatchGetTieredWriteTargets(const std::string &instance_id,
                                                       const std::vector<int64_t> &block_keys,
                                                       std::vector<std::string> &out_targets) const {
    out_targets.assign(block_keys.size(), std::string());
    if (block_keys.empty()) {
        return EC_OK;
    }
    auto indexer = GetIndexer(instance_id);
    if (indexer == nullptr) {
        return EC_INSTANCE_NOT_EXIST;
    }
    RequestContext rc("migration_query_mark_batch");
    KeyVector keys(block_keys.begin(), block_keys.end());
    PropertyMapVector props;
    // 注：部分 block 不存在时 GetProperties 可能返回非 OK 聚合 ec，但 props 仍逐 key 填充
    //（缺失 key 为空 map）。因此不按聚合 ec 早退，直接按 props 逐项解析。
    indexer->GetProperties(&rc, keys, {PROPERTY_TIERED_WRITE_TARGET, PROPERTY_TIERED_WRITE_TS}, props);
    for (size_t i = 0; i < props.size() && i < out_targets.size(); ++i) {
        auto tit = props[i].find(PROPERTY_TIERED_WRITE_TARGET);
        if (tit == props[i].end() || tit->second.empty()) {
            continue;
        }
        auto tsit = props[i].find(PROPERTY_TIERED_WRITE_TS);
        const int64_t ts_ms = (tsit != props[i].end() && !tsit->second.empty()) ? std::atoll(tsit->second.c_str()) : 0;
        if (IsMarkExpired(ts_ms)) {
            continue;
        }
        out_targets[i] = tit->second;
    }
    return EC_OK;
}

ErrorCode MigrationManager::Cancel(int64_t block_key) {
    CopyTaskContext ctx;
    {
        std::lock_guard<std::mutex> lock(task_mutex_);
        auto iter = active_tasks_.find(block_key);
        if (iter == active_tasks_.end()) {
            return EC_NOENT;
        }
        ctx = iter->second;
        active_tasks_.erase(iter);
        UpdateActiveTasksGauge();
    }
    // 清理目标半成品（CAS WRITING->DELETING + 删除）。源端不动。
    SubmitTargetLocationDelete(ctx);
    ClearTieredWriteMark(ctx.instance_id, block_key);
    stat_copy_cancelled_.fetch_add(1, std::memory_order_relaxed);
    KVCM_LOG_INFO("migration cancelled: instance %s block_key %ld dst_loc %s",
                  ctx.instance_id.c_str(),
                  block_key,
                  ctx.dst_location_id.c_str());
    return EC_OK;
}

std::vector<ErrorCode> MigrationManager::BatchCancel(const std::vector<int64_t> &block_keys) {
    std::vector<ErrorCode> results;
    results.reserve(block_keys.size());
    for (int64_t block_key : block_keys) {
        results.push_back(Cancel(block_key));
    }
    return results;
}

bool MigrationManager::HasMigrationTask(int64_t block_key) const {
    std::lock_guard<std::mutex> lock(task_mutex_);
    return active_tasks_.count(block_key) > 0;
}

bool MigrationManager::HasActiveCopyTargetLocation(const std::string &location_id) const {
    if (location_id.empty()) {
        return false;
    }
    std::lock_guard<std::mutex> lock(task_mutex_);
    for (const auto &[_, ctx] : active_tasks_) {
        if (ctx.dst_location_id == location_id) {
            return true;
        }
    }
    return false;
}

size_t MigrationManager::ActiveTaskCount() const {
    std::lock_guard<std::mutex> lock(task_mutex_);
    return active_tasks_.size();
}

std::string MigrationManager::GetActiveTaskDstLocation(int64_t block_key) const {
    std::lock_guard<std::mutex> lock(task_mutex_);
    auto iter = active_tasks_.find(block_key);
    if (iter == active_tasks_.end()) {
        return std::string();
    }
    return iter->second.dst_location_id;
}

void MigrationManager::DebugInsertActiveCopyTask(int64_t block_key, const std::string &dst_location_id) {
    std::lock_guard<std::mutex> lock(task_mutex_);
    CopyTaskContext ctx;
    ctx.block_key = block_key;
    ctx.dst_location_id = dst_location_id;
    active_tasks_[block_key] = std::move(ctx);
}

const CacheLocation *MigrationManager::FindLocationOnStorage(const CacheLocationMap &loc_map,
                                                              const std::string &storage_name,
                                                              std::initializer_list<CacheLocationStatus> statuses) {
    for (const auto &[_, loc_ptr] : loc_map) {
        if (!loc_ptr || std::find(statuses.begin(), statuses.end(), loc_ptr->status()) == statuses.end()) {
            continue;
        }
        for (const auto &spec : loc_ptr->location_specs()) {
            if (DataStorageUri uri(spec.uri()); uri.Valid() && uri.GetHostName() == storage_name) {
                return loc_ptr.get();
            }
        }
    }
    return nullptr;
}

MigrationManager::CopyAdmission MigrationManager::CheckCopyAdmission(int64_t block_key,
                                                                     const CacheLocationMap &loc_map,
                                                                     const std::string &src_storage_name,
                                                                     const std::string &dst_storage_name) const {
    if (HasMigrationTask(block_key)) {
        return {CopyAdmissionStatus::kAlreadyMigrating, nullptr};
    }
    if (FindLocationOnStorage(loc_map, dst_storage_name, {CacheLocationStatus::CLS_SERVING}) != nullptr) {
        return {CopyAdmissionStatus::kTargetServingExists, nullptr};
    }
    if (FindLocationOnStorage(loc_map, dst_storage_name, {CacheLocationStatus::CLS_WRITING}) != nullptr) {
        return {CopyAdmissionStatus::kTargetWritingExists, nullptr};
    }
    const CacheLocation *src_loc =
        FindLocationOnStorage(loc_map, src_storage_name, {CacheLocationStatus::CLS_SERVING});
    if (src_loc == nullptr) {
        return {CopyAdmissionStatus::kSourceServingNotFound, nullptr};
    }
    return {CopyAdmissionStatus::kAccept, src_loc};
}

MigrationManager::MigrationStats MigrationManager::GetStats() const {
    MigrationStats stats;
    stats.copy_submitted = stat_copy_submitted_.load(std::memory_order_relaxed);
    stats.copy_completed = stat_copy_completed_.load(std::memory_order_relaxed);
    stats.copy_failed = stat_copy_failed_.load(std::memory_order_relaxed);
    stats.copy_cancelled = stat_copy_cancelled_.load(std::memory_order_relaxed);
    stats.marks_added = stat_marks_added_.load(std::memory_order_relaxed);
    stats.marks_cleared = stat_marks_cleared_.load(std::memory_order_relaxed);
    stats.marks_expired = stat_marks_expired_.load(std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> lock(task_mutex_);
        stats.active_copy_tasks = active_tasks_.size();
    }
    // 持久化方案下无内存表，active_marks 为 best-effort 近似（added - cleared）。
    const uint64_t added = stats.marks_added;
    const uint64_t cleared = stats.marks_cleared;
    stats.active_marks = added > cleared ? static_cast<size_t>(added - cleared) : 0;
    return stats;
}

} // namespace kv_cache_manager
