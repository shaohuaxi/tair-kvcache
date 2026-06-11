package(default_visibility = ["//visibility:private"])

cc_test(
    name = "CommandLineTest",
    srcs = [
        "command_line_test.cc",
    ],
    copts = ["-fno-access-control"],
    data = [],
    deps = [
        "//kv_cache_manager/common:unittest",
        "//kv_cache_manager/service",
    ],
)

cc_test(
    name = "ServerConfigTest",
    srcs = [
        "server_config_test.cc",
    ],
    copts = ["-fno-access-control"],
    data = [
        "testdata/server_config_simple.conf",
    ],
    deps = [
        "//kv_cache_manager/common:unittest",
        "//kv_cache_manager/service",
    ],
)

cc_test(
    name = "AdminServiceImplTest",
    srcs = [
        "admin_service_impl_test.cc",
    ],
    copts = ["-fno-access-control"],
    data = [],
    deps = [
        "//kv_cache_manager/common:unittest",
        "//kv_cache_manager/manager:cache_manager",
        "//kv_cache_manager/manager:meta_searcher",
        "//kv_cache_manager/manager:migration_manager",
        "//kv_cache_manager/meta",
        "//kv_cache_manager/service:admin_service_impl",
    ],
)
