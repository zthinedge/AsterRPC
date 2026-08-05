#include "asterrpc/trace/TraceContext.h"

#include <algorithm>
#include <random>
#include <utility>

namespace asterrpc::trace{
namespace{

thread_local std::optional<TraceContext> current_context;

std::uint64_t RandomValue(){
    thread_local std::mt19937_64 generator([](){
        std::random_device device;
        std::seed_seq seed{
            device(),
            device(),
            device(),
            device(),
            device(),
            device(),
            device(),
            device()
        };
        return std::mt19937_64(seed);
    }());

    std::uint64_t value=generator();
    while(value==0){
        value=generator();
    }
    return value;
}

std::string HexId(std::size_t word_count){
    constexpr char kHexDigits[]="0123456789abcdef";
    std::string output(word_count*16,'0');

    for(std::size_t index=0;index<word_count;++index){
        std::uint64_t value=RandomValue();
        std::size_t offset=(index+1)*16;

        for(std::size_t digit=0;digit<16;++digit){
            output[offset-digit-1]=kHexDigits[value&0xf];
            value>>=4;
        }
    }

    return output;
}

std::uint64_t ResolveDeadline(
    const TraceContext& parent,
    std::uint64_t deadline_us
){
    if(parent.deadline_us==0){
        return deadline_us;
    }
    if(deadline_us==0){
        return parent.deadline_us;
    }
    return std::min(parent.deadline_us,deadline_us);
}

}

bool TraceContext::IsValid()const noexcept{
    return !trace_id.empty()&&!span_id.empty();
}

TraceContext CreateRootTrace(std::uint64_t deadline_us){
    TraceContext context;
    context.trace_id=HexId(2);
    context.span_id=HexId(1);
    context.deadline_us=deadline_us;
    return context;
}

TraceContext CreateChildSpan(
    const TraceContext& parent,
    std::uint64_t deadline_us
){
    if(!parent.IsValid()){
        return CreateRootTrace(deadline_us);
    }

    TraceContext context;
    context.trace_id=parent.trace_id;
    context.span_id=HexId(1);
    context.parent_span_id=parent.span_id;
    context.deadline_us=ResolveDeadline(parent,deadline_us);
    return context;
}

TraceContext CreateServerSpan(
    std::string trace_id,
    std::string remote_span_id,
    std::uint64_t deadline_us
){
    if(trace_id.empty()){
        return CreateRootTrace(deadline_us);
    }

    TraceContext context;
    context.trace_id=std::move(trace_id);
    context.span_id=HexId(1);
    context.parent_span_id=std::move(remote_span_id);
    context.deadline_us=deadline_us;
    return context;
}

const TraceContext* CurrentTraceContext()noexcept{
    return current_context?&*current_context:nullptr;
}

TraceScope::TraceScope(TraceContext context)
:previous_(std::move(current_context)){
    current_context=std::move(context);
}

TraceScope::~TraceScope(){
    current_context=std::move(previous_);
}

}
