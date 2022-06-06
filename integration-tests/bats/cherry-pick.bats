#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    skip_nbf_dolt_1

    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v varchar(10))"
    dolt add -A
    dolt commit -m "Created table"
    dolt checkout -b branch1
    dolt sql -q "INSERT INTO test VALUES (1, 'a')"
    dolt add -A
    dolt commit -m "Inserted 1"
    dolt sql -q "INSERT INTO test VALUES (2, 'b')"
    dolt add -A
    dolt commit -m "Inserted 2"
    dolt sql -q "INSERT INTO test VALUES (3, 'c')"
    dolt add -A
    dolt commit -m "Inserted 3"
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "cherry-pick: simple cherry pick with the latest commit" {
    run dolt sql -q "SELECT * FROM test" -r csv
    [[ "$output" =~ "1,a" ]] || false
    [[ "$output" =~ "2,b" ]] || false
    [[ "$output" =~ "3,c" ]] || false

    dolt checkout main

    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]

    run dolt sql -q "SELECT * FROM test" -r csv
    [[ ! "$output" =~ "1,a" ]] || false
    [[ ! "$output" =~ "2,b" ]] || false
    [[ "$output" =~ "3,c" ]] || false
}

@test "cherry-pick: commit with multiple row changes" {
    run dolt sql -q "SELECT * FROM test" -r csv
    [[ "$output" =~ "1,a" ]] || false
    [[ "$output" =~ "2,b" ]] || false
    [[ "$output" =~ "3,c" ]] || false

    dolt sql -q "UPDATE test SET v = 'x' WHERE pk = 2"
    dolt sql -q "INSERT INTO test VALUES (5, 'g'), (8, 'u');"
    dolt add -A
    dolt commit -m "Updated 2b to 2x and inserted more rows"

    dolt checkout main

    run dolt cherry-pick branch1~2
    [ "$status" -eq "0" ]

    run dolt sql -q "SELECT * FROM test" -r csv
    [[ "$output" =~ "2,b" ]] || false
    [[ ! "$output" =~ "1,a" ]] || false
    [[ ! "$output" =~ "2,x" ]] || false
    [[ ! "$output" =~ "3,c" ]] || false

    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]

    run dolt sql -q "SELECT * FROM test" -r csv
    [[ ! "$output" =~ "1,a" ]] || false
    [[ "$output" =~ "2,x" ]] || false
    [[ ! "$output" =~ "3,c" ]] || false
    [[ "$output" =~ "5,g" ]] || false
    [[ "$output" =~ "8,u" ]] || false
}

@test "cherry-pick: too far back" {
    dolt checkout main
    run dolt cherry-pick branch1~10
    [ "$status" -eq "1" ]
    [[ "$output" =~ "ancestor" ]] || false
}

@test "cherry-pick: no changes" {
    dolt commit --allow-empty -m "empty commit"
    dolt checkout main
    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]
    [[ "$output" =~ "No changes were made" ]] || false
}

@test "cherry-pick: invalid hash" {
    run dolt cherry-pick aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    [ "$status" -eq "1" ]
    [[ "$output" =~ "target commit not found" ]] || false
}

@test "cherry-pick: has changes in the working set" {
    dolt checkout main
    dolt sql -q "INSERT INTO test VALUES (4, 'f')"
    run dolt cherry-pick branch1~2
    [ "$status" -eq "1" ]
    [[ "$output" =~ "changes" ]] || false
}

@test "cherry-pick: commit with CREATE TABLE" {
    dolt sql -q "CREATE TABLE table_a (pk BIGINT PRIMARY KEY, v varchar(10))"
    dolt sql -q "INSERT INTO table_a VALUES (11, 'aa'), (22, 'ab'), (33, 'ac')"
    dolt sql -q "DELETE FROM test WHERE pk = 2"
    dolt add -A
    dolt commit -m "Added table_a with rows and delete pk=2 from test"

    dolt checkout main

    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]

    dolt sql -q "SHOW TABLES" -r csv
    [[ "$output" =~ "table_a" ]] || false

    run dolt sql -q "SELECT * FROM test" -r csv
    [[ ! "$output" =~ "1,a" ]] || false
    [[ ! "$output" =~ "2,b" ]] || false
    [[ ! "$output" =~ "3,c" ]] || false
}

@test "cherry-pick: commit with DROP TABLE" {
    dolt sql -q "DROP TABLE test"
    dolt add -A
    dolt commit -m "Drop table test"

    run dolt sql -q "SHOW TABLES" -r csv
    [[ ! "$output" =~ "test" ]] || false

    dolt checkout main

    run dolt sql -q "SHOW TABLES" -r csv
    [[ "$output" =~ "test" ]] || false

    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]

    run dolt sql -q "SHOW TABLES" -r csv
    [[ ! "$output" =~ "test" ]] || false
}
