/**
 * Example of FixtureAnyFlatSpec usage
 */

import org.scalatest.Outcome
import org.scalatest.flatspec.FixtureAnyFlatSpec

// Can also pass fixtures into tests with FixtureAnyFlatSpec
class StringSpec extends FixtureAnyFlatSpec {
  type FixtureParam = String // Define the type of the passed fixture object
  override def withFixture(test: OneArgTest): Outcome = {
    // Shared setup (run before each test), including...
    val fixture = "a fixture object" // ...creating a fixture object
    try test(fixture) // Pass the fixture into the test
    finally {
      // Shared cleanup (run at end of each test)
    }
  }
  "The passed fixture" can "be used in the test" in { s => // Fixture passed in as s
    assert(s == "a fixture object")
  }
}