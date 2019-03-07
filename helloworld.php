<html>
<body>
<form action="/sign" method="post">
    <div><textarea name="content" rows="3" cols="60"></textarea></div>
    <div><input type="submit" value="Run"></div>
</form>
<?php

    require __DIR__ . "/vendor/autoload.php";

    use Google\Cloud\BigQuery\BigQueryClient;

    $projectId = "imposing-vista-232806";

    $bigQuery = new BigQueryClient([
        "projectId" => $projectId,
    ]);

    if (array_key_exists("content", $_POST)) {


        $query = <<<ENDSQL
SELECT *
FROM `imposing-vista-232806.cancer.cancer`
LIMIT 10;
ENDSQL;

        //$query = $_POST["content"];
        $queryJobConfig = $bigQuery->query($query);
        $queryResults = $bigQuery->runQuery($queryJobConfig);

        if ($queryResults->isComplete()) {
            $i = 0;
            $rows = $queryResults->rows();
            foreach ($rows as $row) {
                printf("%s<br>\n", implode(", ", array_values($row)));
                //printf('%s,%s,%s<br>' . PHP_EOL, $row['GroupSurv'], $row['TimeSurv'], $row['CensorSurv']);
                $i++;
            }
            printf("Found %s row(s)\n", $i);
        } else {
            printf("The query failed to complete");
            //throw new Exception("The query failed to complete");
        }
    }

?>

</body>
</html>
