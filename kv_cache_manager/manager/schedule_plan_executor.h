#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include "kv_cache_manager/common/error_code.h"
#include "kv_cache_manager/data_storage/data_storage_uri.h"
#include "kv_cache_manager/manager/meta_searcher.h"
#include "kv_cache_manager/metrics/metrics_registry.h"

namespace kv_cache_manager {

#ifndef KVCM_GAUGE_METRICS_FOR_SCHEDULE_PLAN_EXECUTOR
#define KVCM_GAUGE_METRICS_FOR_SCHEDULE_PLAN_EXECUTOR(name)                                                            \
public:                                                                                                                \
    DECLARE_METRICS_NAME_(schedule_plan_executor, name);                                                               \
    DEFINE_GET_METRICS_GAUGE_(schedule_plan_executor, name)                                                            \
                                                                                                                       \
private:                                                                                                               \
    DECLARE_METRICS_GAUGE_(schedule_plan_executor, name);
#endif

class MetaIndexerManager;
class DataStorageManager;

struct CacheMetaDelRequest {
    std::string instance_id;
    std::vector<int64_t> block_keys;
    std::chrono::microseconds delay{std::chrono::seconds(0)};
};

struct PlanExecuteResult {
    ErrorCode status;
    std::string error_message;
};

struct CacheLocationDelRequest {
    std::string instance_id;
    std::vector<int64_t> block_keys;
    std::vector<std::vector<std::string>> location_ids;
    std::chrono::microseconds delay{std::chrono::seconds(0)};
};

// 单个 block 的跨存储复制请求。URI 由上层（MigrationManager）解析与预分配后传入；
// SchedulePlanExecutor 只负责异步调用 backend.Copy 搬字节并回报结果，不创建 location/不做 CAS
// （目标 location 的创建与 CLS_WRITING->SERVING 的状态流转由 MigrationManager 负责）。
struct CacheLocationCopyRequest {
    std::string instance_id;
    int64_t block_key;
    std::string exec_storage_name;        // 执行 Copy 的 backend（一期 = 源 storage 的 unique name）
    std::vector<DataStorageUri> src_uris; // 源端各 spec 的 uri
    std::vector<DataStorageUri> dst_uris; // 目标端各 spec 预分配的 uri（与 src_uris 一一对应）
    std::chrono::microseconds delay{std::chrono::seconds(0)};
};

struct ScheduledTask {
    std::function<void()> task;
    std::chrono::steady_clock::time_point execute_time;
    uint64_t sequence_id;

    bool operator<(const ScheduledTask &other) const {
        if (execute_time != other.execute_time) {
            return execute_time < other.execute_time;
        }
        // ensure strict weak ordering when execute_time is same
        return sequence_id < other.sequence_id;
    }
};

class SchedulePlanExecutor {
public:
    explicit SchedulePlanExecutor(unsigned int thread_count,
                                  const std::shared_ptr<MetaIndexerManager> &meta_manager,
                                  const std::shared_ptr<DataStorageManager> &storage_manager,
                                  const std::shared_ptr<MetricsRegistry> &metrics_registry);
    ~SchedulePlanExecutor();

    std::future<PlanExecuteResult> Submit(const CacheMetaDelRequest &task);
    std::future<PlanExecuteResult> Submit(const CacheLocationDelRequest &task);
    std::future<PlanExecuteResult> Submit(const CacheLocationCopyRequest &task);

    bool SubmitNonBlocking(const CacheMetaDelRequest &req);
    bool SubmitNonBlocking(const CacheLocationDelRequest &req);
    bool SubmitNonBlocking(const CacheLocationCopyRequest &req);

    bool SubmitTask(std::function<void()> task, std::chrono::microseconds delay = std::chrono::microseconds(0));

private:
    std::shared_ptr<MetaIndexerManager> meta_manager_;
    std::shared_ptr<DataStorageManager> data_storage_manager_;
    std::shared_ptr<MetricsRegistry> metrics_registry_;
    std::vector<std::thread> workers_;
    std::atomic<bool> stop_;

    std::multiset<ScheduledTask> tasks_;
    std::mutex queue_mutex_;
    std::condition_variable condition_;
    std::atomic<uint64_t> sequence_counter_{0};

    void WorkerRoutine();

    void Stop();
    bool SubmitRaw(const std::function<void()> &task, std::chrono::microseconds delay);
    static bool FillActualTask(const std::vector<int64_t> &batch_cas_block_keys,
                               const std::vector<std::vector<MetaSearcher::LocationCASTask>> &batch_cas_tasks,
                               const std::vector<std::vector<ErrorCode>> &batch_results,
                               CacheLocationDelRequest &actual_task,
                               std::string &error_message);
    void DoLocationDelTask(const std::shared_ptr<std::promise<PlanExecuteResult>> &promise,
                           const CacheLocationDelRequest &task);
    void DoCopyTask(const std::shared_ptr<std::promise<PlanExecuteResult>> &promise,
                    const CacheLocationCopyRequest &task);

    KVCM_GAUGE_METRICS_FOR_SCHEDULE_PLAN_EXECUTOR(waiting_task_count)
    KVCM_GAUGE_METRICS_FOR_SCHEDULE_PLAN_EXECUTOR(executing_task_count)
};

#undef KVCM_GAUGE_METRICS_FOR_SCHEDULE_PLAN_EXECUTOR

} // namespace kv_cache_manager
