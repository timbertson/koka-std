import std/test
import std/num/int32
import std/num/int64
import std/async/async2
import std/async/timer
import std/time/duration
import std/core-extras
import std/core/unsafe

fun list/push(l, item)
  l ++ [item]

fun listref/push(lr: ref<_, list<a>>, item: a, ?show: (a) -> string)
  println("PUSH: " ++ item.show)
  lr.modify fn(l)
    l := l.push(item)

val short-pause = 1.milli-seconds
val long-pause = 100.milli-seconds

// rbset doesn't have remove, and hashset doesn't work in JS
fun listset/add(l: list<v>, v: v, ?(==): (v, v) -> bool): list<v>
  if l.find(fn(i) i == v).is-just then l
  else Cons(v,l)

fun listset/remove(l: list<v>, v: v, ?(==): (v, v) -> bool): list<v>
  l.list/remove(fn(i) i == v)

fun noleaks(action: () -> <async,io|e> ()): <async,io|e> ()
  println("-------->")
  // NOTE: this doesn't handle multiple resumption
  val pending: ref<global,list<int>> = ref([])

  with finally
    println("<-------- (pending=" ++ (!pending).show ++ ")")
    if ! (!pending).is-empty then
      throw("Test leaked async callbacks")
  
  fun wrap-setup(setup)
    val id = unique()
    println("-> [" ++ id.show ++ "]")

    pending := (!pending).add(id)
    fun done(reason)
      unsafe-total
        println("<- [" ++ id.show ++ ":" ++ reason ++ "]")
      pending := (!pending).remove(id)

    fun wrapped-setup(cb)
      fun wrapped-cb(result, bool)
        done("return")
        cb(result, bool)

      match setup(wrapped-cb)
        Nothing -> Just { done("nodispose") } // TODO should this be Nothing?
        Just(dispose) -> Just { done("cancel"); dispose() }
    wrapped-setup

  with handler<async>
    raw ctl do-await(setup,scope)
      do-await(wrap-setup(setup), scope)
      ()

    fun no-await(setup,scope,f)
      no-await(wrap-setup(setup), scope, f)

    fun async-iox(f)
      async-iox(f)

    fun cancel-scope(scope)
      cancel-scope(scope)

    val async-scope = async-scope

  mask behind<async>(action)

effect audit<a>
  fun record(value: a): ()
  fun records(): list<a>

// fun audit(action: () -> <audit<a>,expect,div|e> b, ?show: (a) -> div string): <div,expect|e> b
fun audit(action: () -> <audit<a>,expect,div,console|e> b, ?show: (a) -> div string): <div,console,expect|e> b
  var log := []
  handle(action)
    fun record(value)
      // TODO `hint` instead of println
      println("record: " ++ value.show)
      log := log.push(value)
      ()
    fun records() -> log

fun error-message(err: error<_>): maybe<string>
  match err
    Error(ex) -> Just(ex.message)
    Ok(_) -> Nothing

fun main(): <io-noexn,async> ()
  with cancel/handle // TODO why isn't this added implicitly?
  run-tests
    effectful-test("simple sleep")
      // with noleaks
      expect(123)
        wait(short-pause)
        123

    effectful-test("interleaved singleton")
      // with noleaks
      expect([1])
        fun action()
          wait(short-pause)
          1
        interleaved([action])

    effectful-test("interleaved singleton which throws")
      with audit
      fun action()
        with finally
          record("finally")
        wait(short-pause)
        throw("error")

      expect(Just("error"))
        try { interleaved([action]) }.error-message

      expect(["finally"]) { records() }

    effectful-test("interleaved pair")
      // with noleaks
      val pair =
        interleaved {
          wait(short-pause)
          1
        } {
          wait(short-pause)
          2
        }
      println("Interleaved: " ++ pair.show)
      expect((1,2)) { pair }

    // // TODO what should cancelable return when canceled?
    // effectful-test("cancel the scope above an interleave")
    //   // with noleaks
    //   with audit
    //   val result = try
    //     with cancelable
    //     val outer-scope = async-scope
    //     interleaved {
    //       wait(short-pause)
    //       outer-scope.cancel()
    //     } {
    //       with finally
    //         record("finally")
    //       on-cancel { println("CANCEL") } {
    //         wait(long-pause)
    //       }
    //       "strand 2 complete"
    //     }
    //   expect(["finally"]) { records() }
    //   expect(((), "nothing?")) { result.untry }
    //   // expect(Just("strand 1 failed")) { result.error-message }

    // // // TODO cancel before anything has spawned, prevents anything from spawning

    effectful-test("interleave with finalization after exn")
      // with noleaks
      with audit
      val result = try
        interleaved {
          wait(short-pause)
          throw("strand 1 failed")
        } {
          with finally
            record("finally")
          on-cancel {
            record("cancel")
          } {
            wait(long-pause)
          }
          "strand 2 complete"
        }
      expect(["cancel", "finally"]) { records() }
      expect(Just("strand 1 failed")) { result.error-message }


    effectful-test("firstof with finalization in slower branch")
      // with noleaks
      with audit
      val result = firstof {
        wait(short-pause)
        1
      } {
        with finally
          record("finally")
        on-cancel {
          record("cancel")
        } {
          wait(long-pause)
        }
        2
      }
      expect(["cancel", "finally"]) { records() }
      expect(1) { result }
      println("Yah!")

    effectful-test("finalization can be async")
      // with noleaks
      val log = ref([]: list<string>)
      fun long-running()
        fun cleanup()
          wait(long-pause)
          log.push("cleanup")
        
        with finally(cleanup)
        log.push("start")
        wait(long-pause) // <- will be canceled
        log.push("wakeup")
        "long"

      fun short-running()
        wait(short-pause)
        log.push("cancel")
        "short"

      val result = firstof(long-running, short-running)
      log.push("result: " ++ result)
      val e = list/(==)(["a"], ["b"])
      expect(["start", "cancel", "cleanup", "result: short"], ?(==) = list/(==)) { !log }

    effectful-test("cancellation and try")
      with audit
      fun interruptible()
        with finally { record("finally") }
        try {
          record("start")
          wait(long-pause)
        } fn(e) {
          record("catch")
        }
        record("after-catch")
        wait(short-pause)
        record("still running...")
        "result"

      expect(Nothing: maybe<string>, ?(==) = maybe/(==))
        timeout(short-pause, interruptible)

      expect(["start", "finally"], ?(==) = list/(==)) { records() }

    effectful-test("error in cancelation is raised")
      fun interruptible()
        with finally { throw("finalization exn") }
        wait(long-pause)
        "result"

      expect("finalization exn")
        try {
          timeout(short-pause, interruptible)
          "success"
        } fn(e) { e.message }

    effectful-test("exception in main code takes priority over error in cancelation")
      fun interruptible()
        with finally { throw("finalization exn") }
        wait(long-pause)
        "main exn"

      expect("finalization exn")
        try {
          firstof(interruptible)
            wait(short-pause)
            throw("main exn")
          "success"
        } fn(e) { e.message }

    effectful-test("uncancelable defers cancelation signal until block is complete")
      val log = ref([]: list<string>)
      fun interruptible()
        defer-cancelation
          wait(long-pause)
          log.push("in uncancelable")
        log.push("after uncancelable")

      expect(Nothing) { timeout(short-pause, interruptible) }
      expect(["in uncancelable"], ?(==) = list/(==)) { !log }
