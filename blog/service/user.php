<?php
require_once "../login.php";
$getUserIdSqlQuery = "SELECT u.userid
FROM user_info u
LEFT JOIN payment_method p ON u.userid = p.userid";

$userIdRes = $connection->query($getUserIdSqlQuery);
$userInfoList = [];
if ($userIdRes->num_rows > 0) {
    while ($row = $userIdRes->fetch_assoc()) {
        if ($row['card_num'] != null) {
            $userInfoList[] = $row;
        }
    }
}

?>