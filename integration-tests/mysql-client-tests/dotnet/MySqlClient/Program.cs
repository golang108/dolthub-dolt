﻿using System;
using System.Data;

using MySql.Data;
using MySql.Data.MySqlClient;

class TestException : Exception
{
    public TestException(string message)
    {
        Console.WriteLine(message);
    }
}

public class DoltSQL
{
    public static int Main(string[] args)
    {
        if (args.Length != 3)
        {
            Console.WriteLine("Must supply $USER $PORT $REPO arguments.");
            return 1;
        }

        var user = args[0];
        var port = args[1];
        var db = args[2];

        string connStr = $"server=127.0.0.1;user={user};database={db};port={port};CharSet=utf8;";
        MySqlConnection conn = new MySqlConnection(connStr);

        try
        {
            conn.Open();
            SetupTest(conn);
            QueryTest(conn);
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
        }

        conn.Close();
        return 0;
    }

    public static void SetupTest(MySqlConnection conn)
    {
        using var cmd = new MySqlCommand();
        cmd.Connection = conn;

        cmd.CommandText = @"CREATE TABLE test (pk int, `value` int, primary key(pk))";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO test(pk, `value`) VALUES(0,0)";
        cmd.ExecuteNonQuery();
    }

    public static void QueryTest(MySqlConnection conn)
    {
        string sql = "SELECT count(*) as count FROM test";
        using (var cmd = new MySqlCommand(sql, conn))
        try
        {
            object result = cmd.ExecuteScalar();
            if (result != null)
            {
                int r = Convert.ToInt32(result);
                if (r != 1)
                {
                        TestException ex = new TestException($"Expected 1, Recieved {r}");
                        throw ex;
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
        }
    }

    public static void DoltSqlTest(MySqlConnection conn)
    {
        string[] queries = new string[] {
            "select dolt_add('-A');",
            "select dolt_commit('-m', 'my commit')",
            "select dolt_checkout('-b', 'mybranch')",
            "insert into test (pk, `value`) values (1,1)",
            "select dolt_commit('-a', '-m', 'my commit2')",
            "select dolt_checkout('main')",
            "select dolt_merge('mybranch')",
        };

        for (int i = 0; i < queries.Length ; i++) {
            try
            {
                var cmd = new MySqlCommand(queries[i], conn);
                cmd.ExecuteScalar();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }

         var finalCmd = new MySqlCommand("select COUNT(*) FROM dolt_log", conn);
         try
         {
             object result = finalCmd.ExecuteScalar();
             if (result != null)
             {
                int r = Convert.ToInt32(result);
                if (r != 3)
                {
                    TestException ex = new TestException($"Expected 3, Recieved {r}");
                    throw ex;
                }
            }
         }
         catch (Exception ex)
         {
             Console.WriteLine(ex.ToString());
         }
    }
}
