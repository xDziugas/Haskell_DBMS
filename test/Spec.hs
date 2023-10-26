import Data.Either
import Data.Maybe ()
import DataFrame (Column (..), ColumnType (..), DataFrame (..), Row, Value (..))
import InMemoryTables qualified as D
import Lib1
import Lib2 (ColumnName (..), Condition (..), ParsedStatement (Select, ShowTable, ShowTables), executeStatement)
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
      Lib2.parseStatement "SELECT MIN flag FROM flags" `shouldBe` Right (Select [ColumnName "flag" (Just Min)] "flags" [])
    it "handles AVG aggregate function" $ do
      Lib2.parseStatement "SELECT AVG id FROM employees" `shouldBe` Right (Select [ColumnName "id" (Just Avg)] "employees" [])
  describe "Lib2.execute" $ do
    it "handles correct show table input" $ do
      Lib2.executeStatement (ShowTable "flags") 
      `shouldBe` Right (DataFrame [Column "flag" StringType, Column "value" BoolType] [[StringValue "a", BoolValue True], [StringValue "b", BoolValue True], [StringValue "b", NullValue], [StringValue "b", BoolValue False]])
    it "handles correct show tables input" $ do
      Lib2.executeStatement ShowTables 
      `shouldBe` Right (DataFrame [Column "Table Names" StringType] [[StringValue "employees"], [StringValue "invalid1"], [StringValue "invalid2"], [StringValue "long_strings"], [StringValue "flags"]])
    it "handles correct SELECT statement without where" $ do
      Lib2.executeStatement (Select [ColumnName "flag" Nothing] "flags" []) 
      `shouldBe` Right (DataFrame [Column "flag" StringType] [[StringValue "a"], [StringValue "b"], [StringValue "b"], [StringValue "b"]])
    it "handles correct SELECT statement with where" $ do
      Lib2.executeStatement (Select [ColumnName "flag" Nothing] "flags" [Equals (ColumnName "flag" Nothing) "b"]) 
      `shouldBe` Right (DataFrame [Column "flag" StringType] [[StringValue "b"], [StringValue "b"], [StringValue "b"]])
    it "handles correct SELECT statement with multiple conditions" $ do
      Lib2.executeStatement (Select [ColumnName "flag" Nothing, ColumnName "value" Nothing] "flags" [Equals (ColumnName "flag" Nothing) "b", Equals (ColumnName "value" Nothing) "true"]) 
      `shouldBe` Right (DataFrame [Column "flag" StringType, Column "value" BoolType] [[StringValue "b", BoolValue True]])
    it "understands * expression" $ do
      Lib2.executeStatement (Select [ColumnName "*" Nothing] "flags" []) 
      `shouldBe` 
      Right (DataFrame [Column "flag" StringType,Column "value" BoolType] [[StringValue "a",BoolValue True],[StringValue "b",BoolValue True],[StringValue "b",NullValue],[StringValue "b",BoolValue False]])
    it "executes agregate" $ do
      Lib2.executeStatement (Select [ColumnName "id" (Just Min)] "employees" []) 
      `shouldBe` Right (DataFrame [Column "minimum" IntegerType] [[IntegerValue 1]])
    it "handles boolean conditions" $ do
      Lib2.executeStatement (Select [ColumnName "flag" Nothing] "flags" [Equals (ColumnName "value" Nothing) "True"]) 
      `shouldBe` Right (DataFrame [Column "flag" StringType] [[StringValue "a"],[StringValue "b"]]) 
    it "executes complex clause" $ do
      Lib2.executeStatement (Select [ColumnName "id" (Just Avg)] "employees" [GreaterEqualThan (ColumnName "id" Nothing) "2"]) 
      `shouldBe` Right (DataFrame [Column "average" IntegerType] [[IntegerValue 2]])