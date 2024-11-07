<!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="UTF-8" />
    <meta http-equiv="X-UA-Compatible" content="ie=edge" />
    <meta name="viewport" content="width-device-width, initial-scale-1.0">
    <title>Connection</title>
</head>

<body>
    <?php
    $hn = 'localhost';
    $db = 'qc_proto';
    $un = 'root';
    $pw = 'mysql';

    $connection = new mysqli($hn, $un, $pw, $db);
    ?>
</body>