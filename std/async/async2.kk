/*---------------------------------------------------------------------------
  Copyright 2012-2021, Microsoft Research, Daan Leijen.

  This is free software; you can redistribute it and/or modify it under the
  terms of the Apache License, Version 2.0. A copy of the License can be
  found in the LICENSE file at the root of this distribution.
---------------------------------------------------------------------------*/

/* Asynchronous primitives

This module is based closely on [@Leijen:async] and aims to have robust and composable asynchronous
primitives. In particular, any outstanding asynchronous operation can be canceled (through `cancel`)
within a certain scope which allows for composable primitives like `timeout` and `firstof`.


## References {-
~ Bibliography { caption:"0"~~ BibItem { #Leijen:async; bibitem-label:"[1]"; searchterm:"Leijen+Daan+Structured+Asynchrony+with+Algebraic+Effects"Daan Leijen.
_Structured Asynchrony with Algebraic Effects_.
Microsoft Research technical report MSR-TR-2017-21, May 2017.
[pdf](https://www.microsoft.com/en-us/research/wp-content/uploads/2017/05/asynceffects-msr-tr-2017-21.pdf)
~~
~
\/
*/
module std/async/async2

import std/data/dict
import std/data/array
import std/num/int32
import std/num/ddouble    // for C# backend
import std/time/duration
import std/async/null
import std/core/unsafe
import std/core/undiv
pub import std/num/ddouble
pub import std/num/float64
import std/time/timestamp
import std/core-extras

  // js is just using primitives

// A type alias for asynchronous operations that can raise exceptions non-deterministically.
// This is common for almost all `:async` operations since `cancel` and `timeout` can
// cancel operations non-deterministically which raises the `Cancel` exception and cancels
// outstanding asynchronous requests.
pub alias asyncx = <async,ndet>

pub alias async-exn = <asyncx,exn>

// the effect used to discontinue execution of the current strand
// (as opposed to the API for triggering cancellation in the current _scope_)
// TODO better name? Don't want to just call it `cancel`
// pub effect cancel
//   final ctl discontinue-execution(): a

// fun cancel/handle(action: () -> <cancel|e> a): e maybe<a>
//   with handler<cancel>
//     final ctl discontinue-execution()
//       return Nothing
//     return(x)
//       return Just(x)
//   action()

// fun on-cancel(action: () -> <cancel|e> a, fin: () -> <cancel|e> ()): <cancel|e> a
//   with handler<cancel>
//     final ctl discontinue-execution()
//       // with finally(discontinue-execution)
//       fin()
//       discontinue-execution()
//   with mask behind<cancel>
//   action()

// ----------------------------------------------------------------------------
// Async effect
// ----------------------------------------------------------------------------
// TODO rename strand-result?
type await-result<a>
  Cancel
  Result(value: a)

type strand-result<a>
  Finalize(y: yield-info)
  Complete(value: a)

fun continue-await(v: await-result<a>, desc: string): async a
  match v
    Result(value) -> value
    Cancel ->
      (unsafe-total { println("[continue-await] " ++ desc ++ ": discontinue") })()
      discontinue()

fun continue-strand(v: strand-result<a>): total a
  match v
    Complete(value) -> value
    Finalize(yield-info) -> unsafe-reyield(yield-info)

alias maybe-dispose-fn = (maybe<() -> io-noexn ()>)

// TODO does this need to be pub?
pub alias await-setup<a> = (cb : (await-result<a>, bool) -> io-noexn ()) -> io-noexn maybe-dispose-fn

// Asynchronous operations have the `:async` effect.
// TODO: async should NOT imply all of io-noexn. We can
// do that by moving the scheduler into an effect rather than an implicit
pub effect async
  // setup a callback and await its completion
  ctl do-await( setup : await-setup<a>, scope : scope) : await-result<a>
  
  // setup a callback to execute `f` on completion, returning control to the caller immediately
  // after the setup is performed
  ctl no-await( setup : await-setup<a>, scope : scope, f : await-result<a> -> io-noexn ()  ) : ()

  // TODO: rename `async-eval` or similar?
  ctl async-iox( action : () -> io-noexn a ) : a
  
  // trigger cancelation of an enclosing scope
  ctl cancel-scope( scope : scope ) : ()

  // // action take on cancellation for outstanding callbacks.
  // // Typically this is just cb(Cancelled, True) however
  // // in an `uncancelable` block it simply sets a flag without invoking `cb`
  // val cancel-mode: cancel-mode

  // react to the cancelation of this strand's scope
  ctl discontinue() : a


// The `cancel` operations cancels any outstanding asynchronous operation under the innermost
// `cancelable` handler. The `cancel` operation itself returns normally
pub fun noscope/cancel-scope() : async ()
  cancel-scope(empty-scope)

type cancel-mode
  Propagate
  Register(r: ref<global, bool>)
  
fun cancel-mode/apply(c: cancel-mode, cb: (await-result<a>, bool) -> io-noexn ()): io-noexn ()
  match c
    Propagate -> cb(Cancel, True)
    Register(r) -> r := True

val cancel-mode/default = Propagate

// defer cancelation until the end of `action`. Note that
// cancelation cannot be ignored, only deferred
// TODO does nested uncancelable defer until the toplevel block is complete?
pub fun uncancelable(action: () -> <st<global>, async|e> a): <st<global>, async|e> a
  val cid = async-iox { unique() }
  var canceled := False
  fun extend(scope : scope )
    parent-scope(cid,scope)

  with handler<async>
    return(x)
      if canceled then
        cancel-scope(empty-scope.extend)
        discontinue()
      else
        x

    fun do-await(setup,scope)  -> do-await(setup,scope.extend)
    fun no-await(setup,scope,f)  -> no-await(setup,scope.extend,f)

    // pass through other operations
    fun cancel-scope(scope) ->
      // mark the current scope as canceled but don't include it
      // in the scope we pass on
      canceled := True
      cancel-scope(scope)

    fun discontinue() -> discontinue()
    fun async-iox(f) -> async-iox(f)
  
  with mask behind<async>
  action()

// ----------------------------------------------------------------------------
// Promises
// ----------------------------------------------------------------------------

// // A _promise_ that carries a value of type `:a`. A promise is initially empty
// // but can be `await`ed  asynchronously until it gets `resolve`d unblocking any
// // `await` operations. After that a promise stays resolved and any `await` will
// // return immediately. It is an error to try to resolve a promise more than once.
// abstract struct promise<a>
//   state : ref<global,promise-state<a>>

// abstract value type promise-state<a>
//   Resolved( value : a )
//   Awaiting( listeners : list<a -> io ()> )

// // Create a new promise.
// pub fun promise() : async promise<a>
//   async-iox { Promise(ref(Awaiting([]))) }

// // Await a promise; returns immediately if the promise was already resolved and otherwise
// // waits asynchronously.
// pub fun promise/await( p : promise<a> ) : asyncx a
//   fun setup(cb : _ -> io-noexn ()): io-noexn ()
//     val r = p.state
//     match (!r)
//       Awaiting(listeners) -> r := Awaiting(Cons(cb,listeners))
//       Resolved(value) -> io-noexn1(cb,value) // resume right away; should not happen due to try-await
//   match p.try-await
//     Just(v) -> v
//     Nothing -> await1(setup)

// // Returns immediately if the promise was already resolved and otherwise return `Nothing`.
// pub fun try-await( p : promise<a> ) : <async,ndet> maybe<a>
//   async-io-noexn
//     val r = p.state
//     match !r
//       Resolved(value) -> Just(value)
//       _ -> Nothing

// // Resolve a promise to `value`. Raises an exception if the promise was already resolved.
// pub fun resolve( p : promise<a>, value : a ) : <asyncx,exn> ()
//   async-io
//     val r = p.state
//     match !r
//       Awaiting(listeners) ->
//         r := Resolved(value)
//         listeners.foreach fn(cbx)  // todo: through set-immediate?
//           cbx(value) // set-immediate1( cbx, value )
//       _ -> throw("Promise was already resolved")

// ----------------------------------------------------------------------------
// Channels
// ----------------------------------------------------------------------------

// A _channel_ of values of type `:a`. Values can be asynchronously `emit`ed into
// a channel, and asynchronously `receive`d.
abstract value struct channel<a>(
  chid : int,
  state : ref<global,channel-state<a>>
)

// todo: use queue data type for the values and listeners for better complexity
abstract type channel-state<a>
  Empty
  Values( value : a, values : list<a> = [] )
  Waiting( listener : a -> io-noexn (), listeners : list<a -> io-noexn ()> = [] )

fun from-values(values : list<a> ) : channel-state<a>
  match values
    Nil -> Empty
    Cons(v,vs)  -> Values(v,vs)

fun from-waiting(listeners : list<a -> io-noexn ()>) : channel-state<a>
  match listeners
    Nil -> Empty
    Cons(l,ls)  -> Waiting(l,ls)

// Create a new asynchronous channel.
pub fun channel() : async channel<a>
  async-iox
    Channel(unique(), ref(Empty))

// Receive (and remove) a value from the channel: returns immediately if a value is available and otherwise
// waits asynchronously until a value becomes available.
fun receive( ch : channel<a>) : <async,ndet> a
  val setup: await-setup<_> = fn(cb : (_,_) -> io-noexn () )
    fun cbr(x) cb(Result(x),True)
    val r = ch.state
    match !r
      Empty -> r := Waiting(cbr,[])
      Waiting(l,ls) -> r := Waiting(l,ls ++ [cbr])
      Values(v,vs)->   // this case should not happen due to `try-receive`
        r := from-values(vs)
        cbr(v)
    Nothing

  match ch.try-receive
    Just(v) -> v
    Nothing -> do-await(setup,empty-scope).continue-await("channel.receive")

// Return immediately if a value is available on the channel and otherwise returns `Nothing`.
pub fun try-receive( ch : channel<a> ) : <async,ndet> maybe<a>
  async-io-noexn
    val r = ch.state
    match (!r)
      Values(v, vs) ->
        r := from-values(vs)
        Just(v)
      _ -> Nothing

fun emit-io( ch : channel<a>, value : a ) : io-noexn ()
  val r = ch.state
  match !r
    Empty -> r := Values(value, [])
    Values(v,vs)  -> r := Values(v,vs ++ [value])
    Waiting(l,ls) ->
      r := from-waiting(ls)
      l(value)

// Emit a value asynchronously into a channel.
pub fun emit( ch : channel<a>, value : a ) : asyncx ()
  async-io-noexn
    emit-io(ch,value)

// fun trace-channel( msg : string, ch : channel<a> ) : <async,ndet> ()
//   async-io-noexn
//     trace-channel-io( msg, ch )

// fun trace-channel-io( msg : string, ch : channel<a> ) : io-noexn ()
//   val msgx = msg ++ ": id=" ++ ch.chid.show
//   val r = ch.state
//   match !r
//     Empty -> trace(msgx ++ ", empty")
//     Values(v,vs) -> trace-any(msgx ++ ", full: " ++ (1 + vs.length).show ++ ": ", v )
//     Waiting(_,ls) -> trace(msgx ++ ", listeners: " ++ (1 + ls.length).show)

// fun trace-anyx( s : string, x : a ) : async ()
//   trace-any(s,x)

// ----------------------------------------------------------------------------
// Asynchronous timeout and waiting
// ----------------------------------------------------------------------------

// Execute `action` but if it is not finished within `secs` seconds duration
// `cancel` it (and return `Nothing`). Due to the generality of `cancel`, this `timeout`
// abstraction can reliably time out over any composition of asynchronous operations
// and is therefore quite expressive.
pub fun timeout( secs : duration, action : () -> <async,io-noexn|e> a, ?set-timeout: (unit-cb, int32) -> io-noexn any, ?clear-timeout: (any) -> io-noexn () ) : <async,io-noexn|e> maybe<a>
  firstof { duration/wait(secs); Nothing} { Just(action()) }

// Execute `a` and `b` interleaved. As soon as one of them finishes,
// `cancel` the other one and return the result of the first.
pub fun firstof( a : () -> <async,io-noexn|e> a, b : () -> <async,io-noexn|e> a) : <async,io-noexn|e> a
  var first := Nothing
  // TODO I think this cancelation is propagating?
  cancelable
    fun run(f)
      with finally { cancel-scope() } // cancel regardless of exn or result
      val result = mask<local> { f() }
      first := Just(result)
      ()

    val _ = interleaved([{run(a)}, {run(b)}]) // this call gets cancelled once the first finishes
    ()

  match first
    Just(x) -> x
    Nothing -> impossible("TODO: can this happen?")

// // Wait (asynchronously) for `secs` seconds as a `:double`.
// // Use `yield()` to yield to other asynchronous operations.
// pub fun float/wait( secs : float64, ?set-timeout: (unit-cb, int32) -> io-noexn any, ?clear-timeout: (any) -> io-noexn () ) : asyncx ()
//   wait(secs.duration)

// Wait (asynchronously) for optional `secs` seconds `:duration` (`= 0.seconds`).
// Use `yield()` to yield generally to other asynchronous operations.
pub fun duration/wait( secs : duration = zero, ?set-timeout: (unit-cb, int32) -> io-noexn any, ?clear-timeout: (any) -> io-noexn () ) : asyncx ()
  if secs <= zero then return yield()
  val msecs = max(zero:int32,secs.milli-seconds.int32)
  await fn(cb)
    val tid = async/set-timeout( fn(){ cb(()) }, msecs )
    Just( { async/clear-timeout(tid) } )

// Yield to other asynchronous operations. Same as `wait(0)`.
pub fun yield(?set-timeout: (unit-cb, int32) -> io-noexn any) : asyncx ()
  await0 fn(cb)
    async/set-timeout( cb, int32/zero )
    ()

// abstract wid for timeout handlers
abstract struct timeout-id(
  timer : any
)

alias unit-cb = () -> io-noexn ()

fun async/set-timeout( cb : unit-cb, ms : int32, ?set-timeout: (unit-cb, int32) -> io-noexn any) : io-noexn timeout-id
  Timeout-id(?set-timeout(cb,max(ms,zero)))


fun async/clear-timeout( tid : timeout-id , ?clear-timeout: (any) -> io-noexn ()) : io-noexn ()
  ?clear-timeout(tid.timer)

// ----------------------------------------------------------------------------
// Interleaved strands of execution
// ----------------------------------------------------------------------------

fun impossible(msg): total a
  unsafe-total
    println("IMPOSSIBLE: " ++ msg)
    exit(1)

// Interleave two actions around their asynchronous operations.
pub fun two/interleaved( action1 : () -> <async,io-noexn|e> a, action2 : () -> <async,io-noexn|e> b ) : <async,io-noexn|e> (a,b)
  fun act1() Left(action1())
  fun act2() Right(action2())
  match interleaved([act1, act2])
    [Left(l), Right(r)] -> (l, r)
    other -> impossible("two/interleaved returned " ++ other.length.show ++ " values")

value struct interleaving<a>
  incomplete: int
  results: list<(int, strand-result<a>)>

fun interleaving/show(i: interleaving<_>)
  "Interleaving(" ++ i.incomplete.show ++ ", results=list-of[" ++ i.results.length.show ++ "]"

fun unsafe-println(s: string)
  unsafe-total { println(s) }

pub fun list/interleaved(xs : list<() -> <async,io-noexn|e> a> ) : <async,io-noexn|e> list<a>
  xs.interleaved-raw.map fn(result)
    // TODO do we fail to finalize/cancel a strand if an ealier one raises an error?
    result.continue-strand

fun list/interleaved-raw(xs : list<() -> <async,io-noexn|e> a> ) : <async,io-noexn|e> list<strand-result<a>>
  var state : some<a> interleaving<a> := Interleaving(xs.length, [])
  with handler<strands<_>>
    return(x)
      unsafe-println("[interleaved] returning with state " ++ state.show)
      state.results.map(fn(tup) tup.snd)
    fun strands-are-busy()
      unsafe-println("[interleaved] strands-are-busy? with state " ++ state.show)
      state.incomplete > 0
    fun strand-done(idx,res)
      state := Interleaving(state.incomplete - 1, state.results.insert(idx,res))
      unsafe-println("[interleaved] strand done; state is now " ++ state.show)
  with mask<local>
  interleave-strands(xs)

// Private effect to keep track of when a strand in an interleaving is done.
// Preferred over using built-in state as this works well if there is an outer handler
// over the state that resumes more than once -- redoing part of the interleaving.
// See `test/algeff/async5.js`
effect strands<a>
  // Are there still strands that need to be resumed?
  fun strands-are-busy() : bool
  // Call this when a strand is done.
  fun strand-done(idx : int, result : strand-result<a>) : ()

// Insert in order with an accumulating list.
fun insert-acc( xs : list<(int,a)>, idx : int, value : a, acc : list<(int,a)> ) : list<(int,a)>
  match xs
    Cons(x,xx) | x.fst < idx -> insert-acc(xx, idx, value, Cons(x,acc))
    _ -> reverse-append( acc, Cons((idx,value),xs) )

// Insert in order
fun insert( xs : list<(int,a)>, idx : int, value : a, n : int  = 0 ) : list<(int,a)>
  if n > 100
    then insert-acc( xs, idx, value, [] )
    else match xs
      Cons(x,xx) | x.fst < idx -> Cons(x, insert(xx, idx, value, n + 1))
      _ -> Cons((idx,value),xs)

inline extern unsafe-no-ndet-div-cast : forall<a,e> (() -> <ndet,div|e> a) -> (() -> e a) 
  inline "#1" 

fun unsafe-no-ndet-div( action : () -> <ndet,div|e> a ) : e a 
  unsafe-no-ndet-div-cast(action)()

// TODO: shouldn't this be covered by effect widening? And what's `h`?
inline extern inject-effects : forall<a,h,e> (() -> e a) -> total (() -> <strands<a>|e> a)
  inline "#1"

// TODO what's the point of `async,div` when async already encapsulates io-noexn?
fun interleave-strands( xs : list<() -> <async,io-noexn|e> a> ) : <async,io-noexn,strands<a>|e> ()
  with cancelable
  val ch : some<a,e> channel<() -> <async|e> a> = channel()
  val yield-on-await = handler<async>
    raw ctl do-await(setup,scope)
      // turn awaits into no-awaits, allowing other strands to execute
      unsafe-println("[interleave-strands]: deferring resumption to channel")
      no-await( setup, scope) fn(res)
        ch.emit-io( /* no-cps */ { rcontext.resume(res) }  )
      ()

    // redirect all other operations
    fun no-await(setup,scope,f)
      no-await(setup,scope,f)
    fun async-iox(f)
      async-iox(f)
    fun cancel-scope(scope)
      cancel-scope(scope)
    fun discontinue()
      discontinue()

  xs.foreach-indexed fn(i, action)
    with yield-on-await
    with mask behind<async>

    val res: strand-result<_> = strand-exec(inject-effects(action))
    println("[interleave-strands] Strand done [" ++ i.show ++ "]; is-finalize = " ++ res.is-finalize.show)
    if res.is-finalize then noscope/cancel-scope() // cancel enclosing scope if finalization happens
    strand-done(i, res)
    ()

  uncancelable
    while { strands-are-busy() } // while there are resumptions on the strands..
      // the only way receive can abort is through a cancelation -- but in that case
      // we should not cancel but instead await all canceled strands; so keep listening on the channel.
      val strand-resume = ch.receive
      strand-resume()
    ()

// // ----------------------------------------------------------------------------
// // Await wrappers
// // ----------------------------------------------------------------------------

// // Convenience function for awaiting a NodeJS style callback where the first argument is a possible exception.
// pub fun await-exn0( setup : (cb : (null<exception>) -> io-noexn () ) -> io-noexn maybe<() -> io-noexn ()> ) : async-exn ()
//   setup/await fn(cb)
//     setup( fn(nexn) cb(Result(nexn.unnull(()))) )
//   .continue-await.untry

// // Convenience function for awaiting a NodeJS style callback where the first argument is a possible exception
// // and the second argument the possible result value.
// pub fun await-exn1( setup : (cb : (null<exception>,a) -> io-noexn () ) -> io-noexn maybe<() -> io-noexn ()> ) : async-exn a
//   setup/await fn(cb)
//     setup( fn(nexn,x) cb(Result(nexn.unnull(x))) )  
//   .continue-await.untry

// fun unnull( nexn : null<exception>, x : a  ) : error<a>
//   match nexn.maybe
//     Nothing -> Ok(x)
//     Just(exn) -> Error(exn)

// Convenience function for awaiting a zero argument callback.
pub fun await0( setup : (cb : () -> io-noexn () ) -> io-noexn () ) : asyncx ()
  setup/await fn(cb)
    setup( fn() cb(Result(())) )
    Nothing
  .continue-await("await0")

// // Convenience function for awaiting a single argument callback.
// pub fun await1( setup : (cb : (a) -> io-noexn () ) -> io-noexn () ) : asyncx a
//   setup/await fn(cb) 
//     setup( fn(x) cb(Result(x)) ) 
//     Nothing 
//   .continue-await


// Primitive: Execute `setup` to set up an asynchronous callback with the host platform. Invoke `cb` as the callback:
// it takes either an exception or a result `a`. Usually `setup` returns `Nothing` but you can return a `Just(cleanup)`
// value where the `cleanup` functions is invoked on cancellation to dispose of any resources (see the implementation of `wait`).
// The callback should be invoked exactly once -- when that happens `await-exn` is resumed with the result.
pub fun setup/await( setup : (cb : (a) -> io-noexn ()) -> io-noexn (maybe<() -> io-noexn ()>) ) : async a
  val result = do-await(fn(cb) { setup(fn(res) cb(Result(res),True)) }, empty-scope)
  // (unsafe-total { println("do-await has RETURNED: cancel=" ++ result.is-cancel.show) })()
  result.continue-await("setup/await")

// // Primitive: Execute `setup` to set up an asynchronous callback with the host platform. Invoke `cb` as the callback: it takes either
// // an exception or a result value, together with boolean parameter whether the callback is done.
// // The callback `cb` will eventually emit the result into the given channel `ch` after applying the transformation `f` to the result.\
// // Note: once you exit the `cancelable` scope where `await-to-channel` was called, the callback is invoked with a `Cancel` exception.
// // The channel should always be awaited within the same `cancelable` scope as the `await-to-channel` invokation.
// // TODO: is this equivalent to ch.emit-io { await(...) }? Why is it its own primitive
// // pub fun await-to-channel( setup : (cb : (error<a>,bool) -> io-noexn ()) -> io-noexn (maybe<() -> io-noexn ()>), ch : channel<b>, f : error<a> -> b  ) : async channel<b>
// //   no-await(fn(cb)
// //     match(try{setup(cb)})
// //       Ok(mcleanup) -> mcleanup
// //       Error(exn) ->
// //         cb(Error(exn),True)
// //         Nothing
// //    , empty-scope,True, fn(res)
// //     ch.emit-io( f(res) )
// //   )
// //   ch

fun async-io-noexn( f : () -> io-noexn a ) : <async,ndet> a
  async-iox(f)

// // Perform an I/O operation at the outer level; exceptions are propagated back.
// fun async-io( f : () -> io a ) : <asyncx,exn> a
//   async-io-noexn( { try(f) } ).untry


// // ----------------------------------------------------------------------------
// // Async handlers: cancelable
// // ----------------------------------------------------------------------------

// inline extern interject-async( action : () -> <async|e> a) : total ( () -> <async,async|e> a)
//   inline "#1"

// Execute `action` in a cancelable scope. If `cancel` is called within `action`,
// any outstanding asynchronous operations started in the cancelable scope are canceled.
// (Outstanding operations outside the cancelable scope are not canceled).
pub fun cancelable( action : () -> <async|e> a ) : <async|e> a
  val cid = async-iox{ unique() }
  fun extend(scope : scope )
    parent-scope(cid,scope)

  handle ({mask behind<async>(action)})
    return(x) ->
      // cancel any outstanding operations still in our scope.
      // this might be needed for `no-await` operations.
      cancel-scope(empty-scope.extend)
      x
    fun do-await(setup,scope)  -> do-await(setup,scope.with-parent(cid))
    fun no-await(setup,scope,f)  -> no-await(setup,scope.extend,f)
    fun cancel-scope(scope)  -> cancel-scope(scope)
    fun async-iox(f)  -> async-iox(f)
    ctl discontinue() ->
      unsafe-println("[cancelable] discontinue. Should I just return?")
      discontinue() // TODO


// // ----------------------------------------------------------------------------
// // Async handle
// // ----------------------------------------------------------------------------

pub fun @default-async(action)
  async/handle(action)

fun noop(): ()
   ()

// The outer `:async` effect handler. This is automatically applied by the compiler
// around the `main` function if it has an `:async` effect.
pub fun async/handle(action : () -> <async,io-noexn> a ) : io-noexn ()
  val cancel-functions : ref<global,list<(scope,() -> <io-noexn|_> ())>> = unsafe-total{ref([])}
  fun handle-await( setup : await-setup<a>, scope : scope, f : await-result<a> -> io-noexn ()) : io-noexn ()
    // println("handle-await")
    val cscope = child-scope(unique(),scope)
    // TODO: is this dispose (always), or only dispose-on-cancel?
    val dispose = ref(noop)
    fun cb( res : await-result<_>, is-done : bool ): io-noexn ()
      println("[async/handle] cb for cscope:" ++ cscope.show)
      if ((!cancel-functions).contains(cscope)) then
        if is-done then
          cancel-functions := (!cancel-functions).remove(cscope)
          if res.is-cancel then
            println("[async/handle] Invoking dispose for scope " ++ cscope.show ++ " because res is cancel")
            (!dispose)()
        f(res)

    trace("[async/handle] ++cscope:" ++ cscope.show)
    val cancel-fn: () -> io-noexn () = fn() cb(Cancel, True)
    cancel-functions := Cons((cscope, cancel-fn), !cancel-functions)
    // setup the callback which cannot throw, and returns a possible dispose function
    match(setup(cb))
      Just(d) -> dispose := d
      Nothing -> ()

  fun handle-cancel( scope : scope ) : io-noexn ()
    // TODO what happens if we try to cancel more than once?
    println("[async/handle] Handle-cancel: " ++ scope.show)
    (!cancel-functions).foreach fn(entry)
      val (cscope,cb) = entry
      if (cscope.in-scope-of(scope)) then cb()

  with handler<async>
    raw ctl do-await(setup, scope)
      handle-await(setup, scope, fn(x) { rcontext.resume(x); () })
    fun no-await(setup, scope, f)
      handle-await(setup, scope, f)
    fun cancel-scope(scope)
      handle-cancel(scope)
    ctl discontinue()
      println("[async/handle] WARN: DISCONTINUE at toplevel, is this valid?")
      exit(1)
      // TODO do we need more cleanup?
      // TODO throw an exception into resume?

    fun async-iox( f )
      f()

  action()
  ()

// // TODO what is this for?
// fun io-noexn( f : () -> io-noexn a ) : io-noexn a
//   f()

// fun io-noexn1( f : (a1) -> io-noexn a, x1 : a1 ) : io-noexn a
//   f(x1)

// ----------------------------------------------------------------------------
// Scope identifiers
// ----------------------------------------------------------------------------

// A scope has an ID, plus a list of child IDs.
// Every created scope has a unique ID
abstract struct scope
  scope-id : int
  child-ids: list<int>

// One scope is considered "in" another if it's equal (same ID), or if
// the child's scope-id is present in the parent's child-ids
val empty-scope = Scope([])

fun with-parent(scope : scope, ) : scope
  match scope
    Scope(id, cids) -> Scope(cid, Cons(cid,cids))
fun parent-scope( cid : int, scope : scope ) : scope
  match scope
    Scope(id, cids) -> Scope(cid, Cons(cid,cids))

fun child-scope( id : int, scope : scope ) : scope
  match scope
    Scope(cids) -> Scope(cids ++ [id])

fun ids/in-scope-of( child : list<int>, parent : list<int> ) : bool
  match parent
    Nil -> True
    Cons(p,ps) -> match child
      Cons(c,cs) -> (c == p && in-scope-of(cs,ps))
      Nil -> False

fun in-scope-of( child : scope, parent : scope ) : bool
  match parent
    Scope(pids) -> match child
      Scope(cids) -> in-scope-of(cids,pids)

fun scope/(==)(scope1 : scope, scope2 : scope ) : bool
  match scope1
    Scope(ids1) -> match scope2
      Scope(ids2) -> ids1==ids2

// Convenience functions for scope maps
fun remove( xs : list<(scope,a)>, scope : scope ) : list<(scope,a)>
  xs.remove( fn(x:(scope,_)) { x.fst == scope })

fun lookup( xs : list<(scope,a)>, scope : scope ) : maybe<a>
  xs.lookup( fn(x:scope) { x == scope })

fun contains( xs : list<(scope,a)>, scope : scope ) : bool
  xs.lookup(scope).bool

fun cscope/show( s : scope ) : string
  match s
    Scope(ids) -> ids.map(show).join("|")



// // abstract extend type exception-info
// //   con Cancel
// //   con Finalize(yld:yield-info)

// // // Was this a cancelation exception?
// // fun exn/is-cancel( exn : exception ) : bool
// //   match exn.info
// //     Cancel -> True
// //     _      -> False

// // // Was this a finalization exception?
// // fun exn/is-finalize(exn : exception) : bool
// //   match exn.info
// //     Finalize -> True
// //     _ -> False

// execute an action, but observe whether it completed normally or
// needs to instead run a finalizer (due to cancellation, exception, etc)
fun strand-exec( action : () -> e a ): e strand-result<a> {
  match unsafe-try-finalize(action)
    Left(fin) ->
      unsafe-total { println("unsafe-try: finalize(fn)") }
      Finalize(fin)
    Right(v) ->
      unsafe-total { println("unsafe-try: Complete(_)") }
      Complete(v)
}

// // fun unsafe-try-all( action : () -> <exn|e> a ) : e error<a>
// //   val fin = unsafe-try-finalize{ try(action) } 
// //   match fin
// //     Right(t)  -> t
// //     Left(yld)  -> Error(Exception("finalize",Finalize(yld)))

// // fun rethrow( exn : exception ) : exn a
// //   match exn.info
// //     Finalize(yld) -> unsafe-reyield(yld)
// //     _             -> throw-exn(exn)
