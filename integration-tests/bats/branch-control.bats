#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
    stop_sql_server
}

@test "branch-control: fresh database. branch control tables exist" {
      run dolt sql -r csv -q "select * from dolt_branch_control"
      [ $status -eq 0 ]
      [ ${lines[0]} = "branch,user,host,permissions" ]
      [ ${lines[1]} = "%,root,localhost,admin" ]
      [ ${lines[2]} = "%,%,%,write" ]

      run dolt sql -q "select * from dolt_branch_namespace_control"
      [ $status -eq 0 ]
      skip "This returns nothing. I think it should return an emopty table ie. names of columns but no data"
}

@test "branch-control: fresh database. branch control tables exist through server interface" {
    start_sql_server

    server_query "dolt_repo_$$" 1 dolt "" "select * from dolt_branch_control" "branch,user,host,permissions\n%,dolt,0.0.0.0,{'admin'}\n%,%,%,{'write'}"

    server_query "dolt_repo_$$" 1 dolt "" "select * from dolt_branch_namespace_control" ""
}

@test "branch-control: modify dolt_branch_control from dolt sql then make sure changes are reflected" {
    dolt sql -q "create user test"
    dolt sql -q "grant all on *.* to test"
    dolt sql -q "delete from dolt_branch_control where user='%'"
    dolt sql -q "insert into dolt_branch_control values ('test', 'test', '%', 'write')"

    run dolt sql -r csv -q "select * from dolt_branch_control"
    [ $status -eq 0 ]
    [ ${lines[0]} = "branch,user,host,permissions" ]
    [ ${lines[1]} = "%,root,localhost,admin" ]
    [ ${lines[2]} = "test,test,%,write" ]

    # Is it weird that the dolt_branch_control can see the dolt user when logged in as test?
    start_sql_server
    server_query "dolt_repo_$$" 1 test "" "select * from dolt_branch_control" "branch,user,host,permissions\n%,dolt,0.0.0.0,{'admin'}\ntest,test,%,{'write'}"
    
}

@test "branch-control: default user root works as expected" {
    # I can't figure out how to get a dolt sql-server started as root.
    # So, I'm copying the pattern from sql-privs.bats and starting it
    # manually.
    PORT=$( definePORT )
    dolt sql-server --host 0.0.0.0 --port=$PORT &
    SERVER_PID=$! # will get killed by teardown_common
    sleep 5 # not using python wait so this works on windows

    skip "This does not return branch permissions for the root user even though I'm connected as root"
    server_query "dolt_repo_$$" 1 root "" "select * from dolt_branch_control" "branch,user,host,permissions\n%,root,localhost,{'admin'}\n%,%,%,{'write'}"
}

