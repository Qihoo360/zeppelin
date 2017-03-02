<?php

$zp = new Zeppelin("127.0.0.1", 9221, "test1");
$times = 10000000;
while($times--) {
  $num = rand(10, 1000);
  $ret = $zp->Set("key1", $num);
  if ($ret == false) {
    echo "Set Error". PHP_EOL;
    break;
  }
  $val = $zp->Get("key1");
  if ($val == false || $val != $num) {
    echo "Error, num: ". $num. " val: ". $val. PHP_EOL;
    var_dump($val);
    break;
  }
  $ret = $zp->Delete("key1");
  if ($ret == false) {
    echo "Delete Error". PHP_EOL;
    break;
  }

  $val = $zp->Get("key1");
  if ($val != false) {
    echo "Error, expect false, but: ". " val: ". $val. PHP_EOL;
    var_dump($val);
    break;
  }

}
