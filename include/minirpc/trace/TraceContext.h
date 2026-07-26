#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace minirpc::trace{

struct TraceContext{
    std::string trace_id;
    std::string span_id;
    std::string parent_span_id;
    std::uint64_t deadline_us=0;

    bool IsValid()const noexcept;
};

TraceContext CreateRootTrace(std::uint64_t deadline_us=0);

TraceContext CreateChildSpan(
    const TraceContext& parent,
    std::uint64_t deadline_us=0
);

TraceContext CreateServerSpan(
    std::string trace_id,
    std::string remote_span_id,
    std::uint64_t deadline_us=0
);

const TraceContext* CurrentTraceContext()noexcept;

class TraceScope{
public:
    explicit TraceScope(TraceContext context);
    ~TraceScope();

    TraceScope(const TraceScope&)=delete;
    TraceScope& operator=(const TraceScope&)=delete;
    TraceScope(TraceScope&&)=delete;
    TraceScope& operator=(TraceScope&&)=delete;

private:
    std::optional<TraceContext> previous_;
};

}
