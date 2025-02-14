package utils

type RdsUrlType struct {
	IsRds        bool
	IsRdsCluster bool
}

var (
	IP_ADDRESS                          RdsUrlType = RdsUrlType{false, false}
	RDS_WRITER_CLUSTER                  RdsUrlType = RdsUrlType{true, true}
	RDS_READER_CLUSTER                  RdsUrlType = RdsUrlType{true, true}
	RDS_CUSTOM_CLUSTER                  RdsUrlType = RdsUrlType{true, true}
	RDS_PROXY                           RdsUrlType = RdsUrlType{true, false}
	RDS_INSTANCE                        RdsUrlType = RdsUrlType{true, false}
	RDS_AURORA_LIMITLESS_DB_SHARD_GROUP RdsUrlType = RdsUrlType{true, false}
	OTHER                               RdsUrlType = RdsUrlType{false, false}
)
