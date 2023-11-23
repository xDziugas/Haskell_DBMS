module Main (main) where

import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Free (Free (..))

import Data.Functor((<&>))
import Data.Time ( UTCTime, getCurrentTime )
import Data.List qualified as L
import Lib1 qualified
import Lib2 qualified
import Lib3 qualified
import InMemoryTables qualified
import DataFrame (DataFrame(..), Column (..), ColumnType (..), Value (..), Row, DataFrame (..))
import Lib2 (parseStatement, ParsedStatement(..), Condition(..), ValueExpr(..))
import System.Console.Repline
  ( CompleterStyle (Word),
    ExitDecision (Exit),
    HaskelineT,
    WordCompleter,
    evalRepl,
  )
import System.Console.Terminal.Size (Window, size, width)

--temp imp
import Data.List (findIndices, findIndex)
import Data.Maybe (fromMaybe)


type TableName = String
type FileContent = String
type ErrorMessage = String

type Repl a = HaskelineT IO a

final :: Repl ExitDecision
final = do
  liftIO $ putStrLn "Goodbye!"
  return Exit

ini :: Repl ()
ini = liftIO $ putStrLn "Welcome to select-manipulate database! Press [TAB] for auto completion."

completer :: (Monad m) => WordCompleter m
completer n = do
  let names = [
              "select", "*", "from", "show", "table",
              "tables", "insert", "into", "values",
              "set", "update", "delete"
              ]
  return $ Prelude.filter (L.isPrefixOf n) names

-- Evaluation : handle each line user inputs
cmd :: String -> Repl ()
cmd c = do
  s <- terminalWidth <$> liftIO size
  result <- liftIO $ cmd' s
  case result of
    Left err -> liftIO $ putStrLn $ "Error: " ++ err
    Right table -> liftIO $ putStrLn table
  where
    terminalWidth :: (Integral n) => Maybe (Window n) -> n
    terminalWidth = maybe 80 width
    cmd' :: Integer -> IO (Either String String)
    cmd' s = do
      df <- runExecuteIO $ Lib3.executeSql c 
      return $ Lib1.renderDataFrameAsTable s <$> df

main :: IO ()
main =
  evalRepl (const $ pure ">>> ") cmd [] Nothing Nothing (Word completer) ini final

runExecuteIO :: Lib3.Execution r -> IO r
runExecuteIO (Pure r) = return r
runExecuteIO (Free step) = do
    next <- runStep step
    runExecuteIO next
    where
        runStep :: Lib3.ExecutionAlgebra a -> IO a
        runStep (Lib3.LoadFile tableName next) = do
            return $ next "Mock for testing"
        runStep (Lib3.ParseStringOfFile fileContent next) = do
            return $ next (Right (DataFrame [] []))

        runStep (Lib3.ExecuteSelect dfs stmt next) = do
            let processedData = executeSelectMock dfs stmt
            return $ next processedData

        runStep (Lib3.GetTime next) = getCurrentTime >>= return . next


        -- Mock SELECT operation
        executeSelectMock :: [DataFrame] -> ParsedStatement -> Either ErrorMessage DataFrame
        executeSelectMock dataFrames stmt = do
            let tableNames = getTableNamesFromStatement stmt  -- Implement this function
            let maybeDataFrames = map (`lookup` InMemoryTables.database) tableNames
            case sequence maybeDataFrames of
                Just dfs -> executeSelectOperation dfs stmt 
                Nothing -> Left "One or more tables not found in InMemoryTables.database"

        getTableNamesFromStatement :: ParsedStatement -> [TableName]
        getTableNamesFromStatement stmt@Select{qeFrom = tableNames} = tableNames

        ------------------- Execute SELECT -------------------
        ------------------------------------------------------
        executeSelectOperation :: [DataFrame] -> ParsedStatement -> Either ErrorMessage DataFrame
        executeSelectOperation dataFrames stmt =
            case stmt of
                Select columnNames tableNames maybeConditions -> do
                    -- Join
                    let conditions = fromMaybe [] maybeConditions
                    let joinedDataFrame = if length dataFrames > 1 
                                          then joinDataFrames dataFrames conditions
                                          else head dataFrames
                    -- Apply conditions
                    let filteredDataFrame = applyConditions joinedDataFrame conditions
                    -- Select columns
                    projectColumns filteredDataFrame columnNames
                _ -> Left "Invalid statement type for selection operation"

        ------------------- Join frames -------------------
        joinDataFrames :: [DataFrame] -> [Condition] -> DataFrame
        joinDataFrames [] _ = DataFrame [] []
        joinDataFrames dfs conditions = foldl1 (joinTwoDataFramesOnCondition conditions) dfs

        joinTwoDataFramesOnCondition :: [Condition] -> DataFrame -> DataFrame -> DataFrame
        joinTwoDataFramesOnCondition conditions (DataFrame cols1 rows1) (DataFrame cols2 rows2) = DataFrame joinedColumns joinedRows
          where
            commonColumns = map (\(Equals colName1 colName2) -> (colName1, colName2)) conditions

            excludedCols2 = filter (\(Column name _) -> notElem name (map snd commonColumns)) cols2

            joinedColumns = cols1 ++ excludedCols2

            joinedRows = [r1 ++ filterRowByColumns r2 cols2 | r1 <- rows1, r2 <- rows2, rowsMatchOnConditions r1 r2 conditions cols1 cols2]

            filterRowByColumns :: Row -> [Column] -> Row
            filterRowByColumns row allCols = 
                map fst . filter (\(_, col) -> elem col excludedCols2) $ zip row allCols

            rowsMatchOnConditions :: Row -> Row -> [Condition] -> [Column] -> [Column] -> Bool
            rowsMatchOnConditions r1 r2 conds cols1 cols2 =
                all (conditionSatisfied r1 r2 cols1 cols2) conds

            conditionSatisfied :: Row -> Row -> [Column] -> [Column] -> Condition -> Bool
            conditionSatisfied r1 r2 cols1 cols2 (Equals colName1 colName2) =
                let val1 = findValueInRow r1 colName1 cols1
                    val2 = findValueInRow r2 colName2 cols2
                in val1 == val2

            findValueInRow :: Row -> String -> [Column] -> Maybe Value
            findValueInRow row colName cols = do
                colIndex <- findIndex (\(Column name _) -> name == colName) cols
                return (row !! colIndex)

        ------------------- Apply conditions -------------------
        applyConditions :: DataFrame -> [Condition] -> DataFrame
        applyConditions df conds = df
        ------------------- Project cols -------------------
        projectColumns :: DataFrame -> [ValueExpr] -> Either ErrorMessage DataFrame
        projectColumns df cols = Right df




                  