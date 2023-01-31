/**
 * Example of AnyFlatSpec usage
 */

import org.scalatest._
import flatspec._

class SetSpec extends AnyFlatSpec {
  override def withFixture(test: NoArgTest): Outcome = { // Define a shared fixture
    // Shared setup (run at beginning of each test)
    try test()
    finally {
      // Shared cleanup (run at end of each test)
    }
  }
  // Define the first test for a subject, in this case: "An empty Set"
  "An empty Set" should "have size 0" in { // Name the subject, write 'should', then the rest of the test name
    assert(Set.empty.isEmpty)            // (Can use 'must' or 'can' instead of 'should')
  }
  it should "produce NoSuchElementException when head is invoked" in { // Define another test for the same
    intercept[NoSuchElementException] {                                // subject with 'it'
      Set.empty.head
    }
  }
  ignore should "be empty" in { // To ignore a test, change 'it' to 'ignore'...
    assert(Set.empty.isEmpty)
  }
  it should "not be non-empty" ignore { // ...or change 'in' to 'ignore'
    assert(Set.empty.isEmpty)
  }
  "A non-empty Set" should "have the correct size" in { // Describe another subject
    assert(Set(1, 2, 3).size == 3)
  }
  // 'it' now refers to 'A non-empty Set'
  it should "return a contained value when head is invoked" is (pending) // Define a pending test
  import tagobjects.Slow
  it should "be non-empty" taggedAs(Slow) in { // Tag a test
    assert(Set(1, 2, 3).nonEmpty)
  }
}
