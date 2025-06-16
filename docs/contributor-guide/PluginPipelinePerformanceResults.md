# Plugin Pipeline Performance Results

## Performance Tests

The failure detection performance tests below will execute a long query, and monitoring will not begin until the `FailureDetectionGraceTime` has passed. A network outage will be triggered after the `NetworkOutageDelayMillis` has passed. The `FailureDetectionInterval` multiplied by the `FailureDetectionCount` represents how long the monitor will take to detect a failure once it starts sending probes to the host. This value combined with the time remaining from `FailureDetectionGraceTime` after the network outage is triggered will result in the expected failure detection time.

For more information please refer to the [failover specific performance tests section](ContributorGuide.md#failover-specific-performance-tests).

### Pgx Enhanced Failure Monitoring Performance with Different Failure Detection Configuration

| FailureDetectionGraceTime | FailureDetectionInterval | FailureDetectionCount | NetworkOutageDelayMillis | MinFailureDetectionTimeMillis | MaxFailureDetectionTimeMillis | AvgFailureDetectionTimeMillis |
| ------------------------- | ------------------------ | --------------------- | ------------------------ | ----------------------------- | ----------------------------- | ----------------------------- |
| 30000                     | 5000                     | 3                     | 5000                     | 38605                         | 38814                         | 38690                         |
| 30000                     | 5000                     | 3                     | 10000                    | 33606                         | 33695                         | 33652                         |
| 30000                     | 5000                     | 3                     | 15000                    | 28612                         | 28664                         | 28640                         |
| 30000                     | 5000                     | 3                     | 20000                    | 23611                         | 23703                         | 23661                         |
| 30000                     | 5000                     | 3                     | 25000                    | 18581                         | 18651                         | 18629                         |
| 30000                     | 5000                     | 3                     | 30000                    | 13652                         | 13710                         | 13685                         |
| 30000                     | 5000                     | 3                     | 35000                    | 13623                         | 13706                         | 13676                         |
| 30000                     | 5000                     | 3                     | 40000                    | 13616                         | 13707                         | 13665                         |
| 30000                     | 5000                     | 3                     | 45000                    | 13647                         | 13707                         | 13673                         |
| 30000                     | 5000                     | 3                     | 50000                    | 13648                         | 13708                         | 13689                         |
| 6000                      | 1000                     | 1                     | 1000                     | 6572                          | 8762                          | 7050                          |
| 6000                      | 1000                     | 1                     | 2000                     | 5572                          | 5739                          | 5666                          |
| 6000                      | 1000                     | 1                     | 3000                     | 4598                          | 4716                          | 4659                          |
| 6000                      | 1000                     | 1                     | 4000                     | 3568                          | 3751                          | 3638                          |
| 6000                      | 1000                     | 1                     | 5000                     | 2564                          | 2660                          | 2622                          |
| 6000                      | 1000                     | 1                     | 6000                     | 1568                          | 1729                          | 1667                          |
| 6000                      | 1000                     | 1                     | 7000                     | 1551                          | 2750                          | 1830                          |
| 6000                      | 1000                     | 1                     | 8000                     | 1610                          | 1720                          | 1661                          |
| 6000                      | 1000                     | 1                     | 9000                     | 1526                          | 1774                          | 1623                          |
| 6000                      | 1000                     | 1                     | 10000                    | 1580                          | 1772                          | 1662                          |

### Pgx Failover Performance with Different Enhanced Failure Monitoring Configuration

| FailureDetectionGraceTime | FailureDetectionInterval | FailureDetectionCount | NetworkOutageDelayMillis | MinFailureDetectionTimeMillis | MaxFailureDetectionTimeMillis | AvgFailureDetectionTimeMillis |
| ------------------------- | ------------------------ | --------------------- | ------------------------ | ----------------------------- | ----------------------------- | ----------------------------- |
| 30000                     | 5000                     | 3                     | 5000                     | 39445                         | 39803                         | 39566                         |
| 30000                     | 5000                     | 3                     | 10000                    | 34348                         | 35272                         | 34605                         |
| 30000                     | 5000                     | 3                     | 15000                    | 29456                         | 29554                         | 29491                         |
| 30000                     | 5000                     | 3                     | 20000                    | 24446                         | 25326                         | 24681                         |
| 30000                     | 5000                     | 3                     | 25000                    | 19311                         | 19821                         | 19499                         |
| 30000                     | 5000                     | 3                     | 30000                    | 14320                         | 15479                         | 14637                         |
| 30000                     | 5000                     | 3                     | 35000                    | 14341                         | 15453                         | 14672                         |
| 30000                     | 5000                     | 3                     | 40000                    | 14322                         | 14519                         | 14423                         |
| 30000                     | 5000                     | 3                     | 45000                    | 14395                         | 15531                         | 14719                         |
| 30000                     | 5000                     | 3                     | 50000                    | 14444                         | 15503                         | 14678                         |
| 6000                      | 1000                     | 1                     | 1000                     | 7330                          | 9847                          | 7907                          |
| 6000                      | 1000                     | 1                     | 2000                     | 6409                          | 6624                          | 6494                          |
| 6000                      | 1000                     | 1                     | 3000                     | 5325                          | 6403                          | 5623                          |
| 6000                      | 1000                     | 1                     | 4000                     | 4358                          | 4526                          | 4439                          |
| 6000                      | 1000                     | 1                     | 5000                     | 3314                          | 4027                          | 3619                          |
| 6000                      | 1000                     | 1                     | 6000                     | 2279                          | 4884                          | 2899                          |
| 6000                      | 1000                     | 1                     | 7000                     | 2345                          | 2513                          | 2428                          |
| 6000                      | 1000                     | 1                     | 8000                     | 2385                          | 2532                          | 2453                          |
| 6000                      | 1000                     | 1                     | 9000                     | 2281                          | 2512                          | 2419                          |
| 6000                      | 1000                     | 1                     | 10000                    | 2410                          | 2511                          | 2456                          |


### MySQL Enhanced Failure Monitoring Performance with Different Failure Detection Configuration

| FailureDetectionGraceTime | FailureDetectionInterval | FailureDetectionCount | NetworkOutageDelayMillis | MinFailureDetectionTimeMillis | MaxFailureDetectionTimeMillis | AvgFailureDetectionTimeMillis |
| ------------------------- | ------------------------ | --------------------- | ------------------------ | ----------------------------- | ----------------------------- | ----------------------------- |
| 30000                     | 5000                     | 3                     | 5000                     | 38404                         | 38935                         | 38524                         |
| 30000                     | 5000                     | 3                     | 10000                    | 33438                         | 33480                         | 33461                         |
| 30000                     | 5000                     | 3                     | 15000                    | 28406                         | 28473                         | 28451                         |
| 30000                     | 5000                     | 3                     | 20000                    | 23432                         | 23491                         | 23464                         |
| 30000                     | 5000                     | 3                     | 25000                    | 18465                         | 18497                         | 18482                         |
| 30000                     | 5000                     | 3                     | 30000                    | 13407                         | 13465                         | 13442                         |
| 30000                     | 5000                     | 3                     | 35000                    | 13413                         | 13559                         | 13470                         |
| 30000                     | 5000                     | 3                     | 40000                    | 13393                         | 13482                         | 13448                         |
| 30000                     | 5000                     | 3                     | 45000                    | 13435                         | 13493                         | 13463                         |
| 30000                     | 5000                     | 3                     | 50000                    | 13412                         | 13478                         | 13436                         |
| 6000                      | 1000                     | 1                     | 1000                     | 6343                          | 8885                          | 6882                          |
| 6000                      | 1000                     | 1                     | 2000                     | 5351                          | 5484                          | 5414                          |
| 6000                      | 1000                     | 1                     | 3000                     | 4389                          | 4510                          | 4451                          |
| 6000                      | 1000                     | 1                     | 4000                     | 3359                          | 3499                          | 3417                          |
| 6000                      | 1000                     | 1                     | 5000                     | 2341                          | 2525                          | 2404                          |
| 6000                      | 1000                     | 1                     | 6000                     | 1393                          | 1469                          | 1426                          |
| 6000                      | 1000                     | 1                     | 7000                     | 1428                          | 1519                          | 1456                          |
| 6000                      | 1000                     | 1                     | 8000                     | 1382                          | 1514                          | 1445                          |
| 6000                      | 1000                     | 1                     | 9000                     | 1388                          | 1533                          | 1444                          |
| 6000                      | 1000                     | 1                     | 10000                    | 1406                          | 1520                          | 1459                          |

### MySQL Failover Performance with Different Enhanced Failure Monitoring Configuration

| FailureDetectionGraceTime | FailureDetectionInterval | FailureDetectionCount | NetworkOutageDelayMillis | MinFailureDetectionTimeMillis | MaxFailureDetectionTimeMillis | AvgFailureDetectionTimeMillis |
| ------------------------- | ------------------------ | --------------------- | ------------------------ | ----------------------------- | ----------------------------- | ----------------------------- |
| 30000                     | 5000                     | 3                     | 5000                     | 39237                         | 39498                         | 39334                         |
| 30000                     | 5000                     | 3                     | 10000                    | 33976                         | 35714                         | 34512                         |
| 30000                     | 5000                     | 3                     | 15000                    | 29198                         | 29430                         | 29282                         |
| 30000                     | 5000                     | 3                     | 20000                    | 23857                         | 25768                         | 24539                         |
| 30000                     | 5000                     | 3                     | 25000                    | 19215                         | 19424                         | 19299                         |
| 30000                     | 5000                     | 3                     | 30000                    | 13982                         | 15756                         | 14516                         |
| 30000                     | 5000                     | 3                     | 35000                    | 14271                         | 15967                         | 14661                         |
| 30000                     | 5000                     | 3                     | 40000                    | 13929                         | 14318                         | 14223                         |
| 30000                     | 5000                     | 3                     | 45000                    | 13857                         | 15769                         | 14511                         |
| 30000                     | 5000                     | 3                     | 50000                    | 14000                         | 15797                         | 14577                         |
| 6000                      | 1000                     | 1                     | 1000                     | 7217                          | 9493                          | 7710                          |
| 6000                      | 1000                     | 1                     | 2000                     | 6242                          | 6306                          | 6284                          |
| 6000                      | 1000                     | 1                     | 3000                     | 5243                          | 6743                          | 5562                          |
| 6000                      | 1000                     | 1                     | 4000                     | 3807                          | 4316                          | 4198                          |
| 6000                      | 1000                     | 1                     | 5000                     | 3188                          | 3366                          | 3295                          |
| 6000                      | 1000                     | 1                     | 6000                     | 2206                          | 2385                          | 2281                          |
| 6000                      | 1000                     | 1                     | 7000                     | 2201                          | 3442                          | 2529                          |
| 6000                      | 1000                     | 1                     | 8000                     | 2236                          | 2378                          | 2313                          |
| 6000                      | 1000                     | 1                     | 9000                     | 2285                          | 2360                          | 2327                          |
| 6000                      | 1000                     | 1                     | 10000                    | 2244                          | 2377                          | 2313                          |

## Plugin Benchmarks

The plugin benchmarks measure the performance overhead of different plugin operations with varying numbers of plugins in the pipeline. Each benchmark ran with 0, 1, 2, 5, and 10 plugins to demonstrate how performance scales with plugin count. We have identified that there is a larger overhead than what is desired and we plan on reducing this overhead in the future.

### Plugin Manager Operation Benchmarks

| Operation                       | ns/op  | B/op | allocs/op |
| ------------------------------- | ------ | ---- | --------- |
| Connect-0                       | 739.8  | 320  | 8         |
| Connect-1                       | 1205   | 488  | 11        |
| Connect-2                       | 1605   | 662  | 14        |
| Connect-5                       | 2640   | 1208 | 23        |
| Connect-10                      | 5920   | 2077 | 38        |
| Execute-0                       | 437.1  | 208  | 5         |
| Execute-1                       | 855.7  | 387  | 8         |
| Execute-2                       | 1256   | 544  | 11        |
| Execute-5                       | 2266   | 1008 | 20        |
| Execute-10                      | 4218   | 1937 | 35        |
| InitHostProvider-0              | 125.1  | 64   | 2         |
| InitHostProvider-1              | 258.7  | 180  | 3         |
| InitHostProvider-2              | 274.3  | 195  | 3         |
| InitHostProvider-5              | 267.7  | 195  | 3         |
| InitHostProvider-10             | 253.5  | 176  | 3         |
| NotifyConnectionChanged-0       | 227.5  | 272  | 3         |
| NotifyConnectionChanged-1       | 283.1  | 288  | 4         |
| NotifyConnectionChanged-2       | 341.4  | 304  | 5         |
| NotifyConnectionChanged-5       | 532.9  | 352  | 8         |
| NotifyConnectionChanged-10      | 812.3  | 432  | 13        |
| ReleaseResources-0              | 396.6  | 48   | 1         |
| ReleaseResources-1              | 540.9  | 141  | 1         |
| ReleaseResources-2              | 638.2  | 227  | 1         |
| ReleaseResources-5              | 891.0  | 534  | 1         |
| ReleaseResources-10             | 1228   | 928  | 1         |
