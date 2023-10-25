import Data.Either
import Data.Maybe ()
import InMemoryTables qualified as D
import Lib1
import Lib2 (ColumnName (..), Condition (..), ParsedStatement (Select, ShowTable, ShowTables))
import Lib2 qualified
import Test.Hspec

main :: IO ()
main = hspec $ do
  describe "Lib2.showtable" $ do
    it "handles correct input" $ do
      Lib2.parseStatement "show table flags" `shouldBe` Right (ShowTable "flags")
    it "handles incorrect names" $ do
      Lib2.parseStatement "show table asdf" `shouldBe` Left "Invalid show statement: after SHOW keyword no keyword TABLE or TABLES was found"
    it "handles incorrect number of names" $ do
      Lib2.parseStatement "show table flags asdf" `shouldBe` Left "Invalid show statement: there can only be one table"
    it "handles case-insensitive keywords" $ do
      Lib2.parseStatement "sHoW TaBlE flags" `shouldBe` Right (ShowTable "flags")
    it "handles case-sensitive names" $ do
      Lib2.parseStatement "sHoW TaBlE flAgS" `shouldBe` Left "Invalid show statement: no column found with that name"
  describe "Lib2.showtables" $ do
    it "handles correct input" $ do
      Lib2.parseStatement "show tables" `shouldBe` Right ShowTables
    it "handles incorrect input" $ do
      Lib2.parseStatement "show tables asdf" `shouldBe` Left ""
    it "handles case-insensitive keywords" $ do
      Lib2.parseStatement "shOW tAblEs" `shouldBe` Right ShowTables
  describe "Lib2.Select" $ do
    it "handles correct input" $ do
      Lib2.parseStatement "SELECT flag FROM flags" `shouldBe` Right (Select [ColumnName "flag" Nothing] "flags" [])
    it "handles correct input with WHERE keyword" $ do
      Lib2.parseStatement "SELECT flag FROM flags WHERE flag = b" `shouldBe` Right (Select [ColumnName "flag" Nothing] "flags" [Equals (ColumnName "flag" Nothing) "b"])
    it "handles multiple conditions in correct input" $ do
      Lib2.parseStatement "SELECT flag, value FROM flags WHERE flag = b AND value = true" `shouldBe` Right (Select [ColumnName "flag" Nothing, ColumnName "value" Nothing] "flags" [Equals (ColumnName "flag" Nothing) "b", Equals (ColumnName "value" Nothing) "true"])
    it "handles MIN aggregate function" $ do
      Lib2.parseStatement "SELECT MIN flag FROM flags" `shouldBe` Right (Select [ColumnName "flag" Nothing] "flags" [Min])
    it "handles AVG aggregate function" $ do
      Lib2.parseStatement "SELECT AVG id FROM employees" `shouldBe` Right (Select [ColumnName "id" Nothing] "employees" [Avg])
