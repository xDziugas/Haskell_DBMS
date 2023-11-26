import Data.Either
import Data.Maybe ()
import DataFrame (Column (..), ColumnType (..), DataFrame (..), Row, Value (..))
import InMemoryTables qualified as D
import Lib1
import Lib3 (executeSqlTest, runExecuteIOTest)
import Lib2 qualified
import Test.Hspec

main :: IO ()
main = hspec $ do
  describe "NOW Operations" $ do
    it "correctly displays the time" $ do
      let query = "now();"
      let expected = Right (DataFrame [Column "current_time" StringType] [[StringValue "2000-01-01 12:00:00"]])
      result <- runExecuteIOTest $ Lib3.executeSqlTest query
      result `shouldBe` expected
    it "correctly displays the time using case-sensitive letters" $ do
      let query = "NoW();"
      let expected = Right (DataFrame [Column "current_time" StringType] [[StringValue "2000-01-01 12:00:00"]])
      result <- runExecuteIOTest $ Lib3.executeSqlTest query
      result `shouldBe` expected
    it "correctly handles whitespace" $ do
      let query = "now();  "
      let expected = Right (DataFrame [Column "current_time" StringType] [[StringValue "2000-01-01 12:00:00"]])
      result <- runExecuteIOTest $ Lib3.executeSqlTest query
      result `shouldBe` expected
  describe "UPDATE operations" $ do
    it "correctly updates the table when the condition is met" $ do
      let query = "update employees set id = 2 where surname = \"Po\";"
      let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] 
                                      [[IntegerValue 2, StringValue "Vi", StringValue "Po"], 
                                       [IntegerValue 2, StringValue "Ed", StringValue "Dl"]])
      result <- runExecuteIOTest $ Lib3.executeSqlTest query
      result `shouldBe` expected
    it "correctly handles whitespace" $ do
      let query = "update employees set id = 2 where surname = \"Po\";   "
      let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] 
                                      [[IntegerValue 2, StringValue "Vi", StringValue "Po"], 
                                       [IntegerValue 2, StringValue "Ed", StringValue "Dl"]])
      result <- runExecuteIOTest $ Lib3.executeSqlTest query
      result `shouldBe` expected
    it "correctly updates the table when the condition is met using case-sensitive letters" $ do
      let query = "uPDatE employees set id = 2 where surname = \"Po\";"
      let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] 
                                      [[IntegerValue 2, StringValue "Vi", StringValue "Po"], 
                                       [IntegerValue 2, StringValue "Ed", StringValue "Dl"]])
      result <- runExecuteIOTest $ Lib3.executeSqlTest query
      result `shouldBe` expected
    it "leaves the table unchanged when the condition is not met" $ do
      let query = "update employees set id = 2 where surname = \"Nonexistent\";"
      let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
                                      [[IntegerValue 1, StringValue "Vi", StringValue "Po"],
                                       [IntegerValue 2, StringValue "Ed", StringValue "Dl"]])
      result <- runExecuteIOTest $ Lib3.executeSqlTest query
      result `shouldBe` expected
    it "updates multiple rows when the condition is met for each" $ do
        let query = "update employees set id = 2 where id > 1;"
        let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] 
                                        [[IntegerValue 1, StringValue "Vi", StringValue "Po"],
                                         [IntegerValue 2, StringValue "Ed", StringValue "Dl"]])
        result <- runExecuteIOTest $ Lib3.executeSqlTest query
        result `shouldBe` expected
    it "handles updates with multiple set conditions" $ do
        let query = "update employees set id = 2, name = \"Updated\" where id = 1;"
        let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
                                        [[IntegerValue 2, StringValue "Updated", StringValue "Po"],
                                         [IntegerValue 2, StringValue "Ed", StringValue "Dl"]])
        result <- runExecuteIOTest $ Lib3.executeSqlTest query
        result `shouldBe` expected
    it "correctly updates boolean values" $ do
        let query = "update tasks set task_id = 601 where is_completed = False;"
        let expected = Right (DataFrame [Column "task_id" IntegerType, Column "description" StringType, Column "is_completed" BoolType]
                                        [[IntegerValue 601, StringValue "Setup project environment", BoolValue False],
                                         [IntegerValue 602, StringValue "Complete initial documentation", BoolValue True],
                                         [IntegerValue 601, StringValue "Implement core features", BoolValue False],
                                         [IntegerValue 601, StringValue "Conduct code review", BoolValue False], 
                                         [IntegerValue 605, StringValue "Prepare for deployment", BoolValue True]])
        result <- runExecuteIOTest $ Lib3.executeSqlTest query
        result `shouldBe` expected
    it "handles mismatched data types in WHERE clause" $ do
        let query = "update non_existant_table set id = \"1\" where surname = \"Po\";"
        let expected = Left "Type mismatch for column: task_id"
        result <- runExecuteIOTest $ Lib3.executeSqlTest query
        result `shouldBe` expected
    it "handles non-existent table in query" $ do
        let query = "update non_existant_table set id = 1 where surname = \"Po\";"
        let expected = Left "Table not found in InMemoryTables"
        result <- runExecuteIOTest $ Lib3.executeSqlTest query
        result `shouldBe` expected
    it "handles invalid column name in query" $ do
        let query = "update employees set ids = 3 where id = 1;"
        let expected = Left "Column not found: ids"
        result <- runExecuteIOTest $ Lib3.executeSqlTest query
        result `shouldBe` expected

    

