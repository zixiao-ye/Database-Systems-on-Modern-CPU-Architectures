#include "moderndbs/codegen/expression.h"
#include "moderndbs/error.h"

using JIT = moderndbs::JIT;
using Expression = moderndbs::Expression;
using ExpressionCompiler = moderndbs::ExpressionCompiler;
using data64_t = moderndbs::data64_t;

/// Evaluate the expresion.
data64_t Expression::evaluate(const data64_t* /*args*/) {
    throw NotImplementedException();
}

/// Build the expression
llvm::Value* Expression::build(llvm::IRBuilder<>& /*builder*/,
                               llvm::Value* /*args*/) {
    throw NotImplementedException();
}

/// Constructor.
ExpressionCompiler::ExpressionCompiler(llvm::orc::ThreadSafeContext& context)
    : context(context), module(std::make_unique<llvm::Module>(
                            "meaningful_module_name", *context.getContext())),
      jit(context), fnPtr(nullptr) {}

/// Compile an expression.
void ExpressionCompiler::compile(Expression& expression, bool verbose) {
    auto& ctx = *context.getContext();
    llvm::IRBuilder<> builder(ctx);

    // A function *definition* for our own
    // define i64 @foo(i64, ...) {
    auto fooT = llvm::FunctionType::get(
        llvm::Type::getInt64Ty(ctx),
        {llvm::Type::getInt64Ty(ctx)->getPointerTo()}, false);
    auto fooFn = llvm::cast<llvm::Function>(
        module->getOrInsertFunction("foo", fooT).getCallee());

    // Then, we generate the function logic.
    llvm::BasicBlock* fooFnEntryBlock =
        llvm::BasicBlock::Create(ctx, "entry", fooFn);
    builder.SetInsertPoint(fooFnEntryBlock);
    std::vector<llvm::Value*> fooFnArgs;
    for (llvm::Function::arg_iterator ai = fooFn->arg_begin();
         ai != fooFn->arg_end(); ++ai) {
        fooFnArgs.push_back(&*ai);
    }

    llvm::Value* fooFnTmp = expression.build(builder, fooFn->arg_begin());
    auto tmp = builder.CreateBitCast(fooFnTmp, builder.getInt64Ty());

    // ret i64 %tmp
    builder.CreateRet(tmp);

    // Dump the module (useful for debugging)
    if (verbose) {
        module->print(llvm::errs(), nullptr);
    }

    auto err = jit.addModule(std::move(module));
    if (err) {
        throw std::runtime_error("jit addModule failed!");
    }

    fnPtr = reinterpret_cast<decltype(fnPtr)>(jit.getPointerToFunction("foo"));
}

/// Compile an expression.
data64_t ExpressionCompiler::run(data64_t* args) { return fnPtr(args); }
