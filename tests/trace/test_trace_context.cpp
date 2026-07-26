#include "minirpc/trace/TraceContext.h"

#include <cassert>
#include <cctype>
#include <cstdint>
#include <string>

namespace{

using minirpc::trace::CreateChildSpan;
using minirpc::trace::CreateRootTrace;
using minirpc::trace::CreateServerSpan;
using minirpc::trace::CurrentTraceContext;
using minirpc::trace::TraceContext;
using minirpc::trace::TraceScope;

bool IsLowerHex(const std::string& value){
    for(unsigned char character:value){
        if(!std::isdigit(character)&&
           !(character>='a'&&character<='f')){
            return false;
        }
    }
    return true;
}

void TestIdGeneration(){
    TraceContext root=CreateRootTrace(1000);
    assert(root.IsValid());
    assert(root.trace_id.size()==32);
    assert(root.span_id.size()==16);
    assert(root.parent_span_id.empty());
    assert(root.deadline_us==1000);
    assert(IsLowerHex(root.trace_id));
    assert(IsLowerHex(root.span_id));

    TraceContext another=CreateRootTrace();
    assert(another.trace_id!=root.trace_id);
    assert(another.span_id!=root.span_id);
}

void TestParentAndDeadlinePropagation(){
    TraceContext root=CreateRootTrace(1000);
    TraceContext child=CreateChildSpan(root,2000);

    assert(child.trace_id==root.trace_id);
    assert(child.span_id!=root.span_id);
    assert(child.parent_span_id==root.span_id);
    assert(child.deadline_us==1000);

    TraceContext tighter=CreateChildSpan(root,500);
    assert(tighter.deadline_us==500);

    TraceContext server=CreateServerSpan(
        child.trace_id,
        child.span_id,
        child.deadline_us
    );
    assert(server.trace_id==root.trace_id);
    assert(server.parent_span_id==child.span_id);
    assert(server.span_id!=child.span_id);
    assert(server.deadline_us==1000);
}

void TestScopeRestoresPreviousContext(){
    assert(CurrentTraceContext()==nullptr);
    TraceContext root=CreateRootTrace();

    {
        TraceScope root_scope(root);
        assert(CurrentTraceContext()!=nullptr);
        assert(CurrentTraceContext()->span_id==root.span_id);

        TraceContext child=CreateChildSpan(*CurrentTraceContext());
        {
            TraceScope child_scope(child);
            assert(CurrentTraceContext()->span_id==child.span_id);
        }

        assert(CurrentTraceContext()->span_id==root.span_id);
    }

    assert(CurrentTraceContext()==nullptr);
}

}

int main(){
    TestIdGeneration();
    TestParentAndDeadlinePropagation();
    TestScopeRestoresPreviousContext();
    return 0;
}
