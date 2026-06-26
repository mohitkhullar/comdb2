/* Copyright 2015 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License. */
package com.bloomberg.comdb2.jdbc;

import static org.junit.Assert.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CArrayTest {

    static String db;
    static String cluster;
    Connection conn;

    @Before
    public void setUp() throws SQLException {
        db = System.getProperty("cdb2jdbc.test.database");
        cluster = System.getProperty("cdb2jdbc.test.cluster");
        conn = DriverManager.getConnection(String.format("jdbc:comdb2://%s/%s", cluster, db));
    }

    @After
    public void tearDown() throws SQLException {
        if (conn != null)
            conn.close();
    }

    /* --- Typed setter API tests (setInt32Array, setInt64Array, etc.) --- */

    @Test
    public void testInt32Array() throws SQLException {
        Comdb2PreparedStatement ps = (Comdb2PreparedStatement)
                conn.prepareStatement("SELECT value FROM carray(?)");
        ps.setInt32Array(1, new int[]{10, 20, 30});
        ResultSet rs = ps.executeQuery();

        List<Integer> results = new ArrayList<>();
        while (rs.next())
            results.add(rs.getInt(1));

        assertEquals(3, results.size());
        assertEquals(Integer.valueOf(10), results.get(0));
        assertEquals(Integer.valueOf(20), results.get(1));
        assertEquals(Integer.valueOf(30), results.get(2));
        rs.close();
        ps.close();
    }

    @Test
    public void testInt64Array() throws SQLException {
        Comdb2PreparedStatement ps = (Comdb2PreparedStatement)
                conn.prepareStatement("SELECT value FROM carray(?)");
        ps.setInt64Array(1, new long[]{100L, 200L, 300L});
        ResultSet rs = ps.executeQuery();

        List<Long> results = new ArrayList<>();
        while (rs.next())
            results.add(rs.getLong(1));

        assertEquals(3, results.size());
        assertEquals(Long.valueOf(100), results.get(0));
        assertEquals(Long.valueOf(200), results.get(1));
        assertEquals(Long.valueOf(300), results.get(2));
        rs.close();
        ps.close();
    }

    @Test
    public void testDoubleArray() throws SQLException {
        Comdb2PreparedStatement ps = (Comdb2PreparedStatement)
                conn.prepareStatement("SELECT value FROM carray(?)");
        ps.setDoubleArray(1, new double[]{1.1, 2.2, 3.3});
        ResultSet rs = ps.executeQuery();

        List<Double> results = new ArrayList<>();
        while (rs.next())
            results.add(rs.getDouble(1));

        assertEquals(3, results.size());
        assertEquals(1.1, results.get(0), 0.001);
        assertEquals(2.2, results.get(1), 0.001);
        assertEquals(3.3, results.get(2), 0.001);
        rs.close();
        ps.close();
    }

    @Test
    public void testTextArray() throws SQLException {
        Comdb2PreparedStatement ps = (Comdb2PreparedStatement)
                conn.prepareStatement("SELECT value FROM carray(?)");
        ps.setTextArray(1, new String[]{"hello", "world", "foo"});
        ResultSet rs = ps.executeQuery();

        List<String> results = new ArrayList<>();
        while (rs.next())
            results.add(rs.getString(1));

        assertEquals(3, results.size());
        assertEquals("hello", results.get(0));
        assertEquals("world", results.get(1));
        assertEquals("foo", results.get(2));
        rs.close();
        ps.close();
    }

    @Test
    public void testBlobArray() throws SQLException {
        byte[] b1 = new byte[]{0x01, 0x02};
        byte[] b2 = new byte[]{0x03, 0x04, 0x05};
        Comdb2PreparedStatement ps = (Comdb2PreparedStatement)
                conn.prepareStatement("SELECT value FROM carray(?)");
        ps.setBlobArray(1, new byte[][]{b1, b2});
        ResultSet rs = ps.executeQuery();

        List<byte[]> results = new ArrayList<>();
        while (rs.next())
            results.add(rs.getBytes(1));

        assertEquals(2, results.size());
        assertArrayEquals(b1, results.get(0));
        assertArrayEquals(b2, results.get(1));
        rs.close();
        ps.close();
    }

    /* --- Standard JDBC API tests (createArrayOf + setArray) --- */

    @Test
    public void testCreateArrayOfAndSetArray() throws SQLException {
        Array arr = conn.createArrayOf("int32", new Object[]{10, 20, 30});
        PreparedStatement ps = conn.prepareStatement("SELECT value FROM carray(?)");
        ps.setArray(1, arr);
        ResultSet rs = ps.executeQuery();

        List<Integer> results = new ArrayList<>();
        while (rs.next())
            results.add(rs.getInt(1));

        assertEquals(3, results.size());
        assertEquals(Integer.valueOf(10), results.get(0));
        assertEquals(Integer.valueOf(20), results.get(1));
        assertEquals(Integer.valueOf(30), results.get(2));
        rs.close();
        ps.close();
    }

    @Test
    public void testSetObjectWithArray() throws SQLException {
        Array arr = conn.createArrayOf("int64", new Object[]{42L, 43L});
        PreparedStatement ps = conn.prepareStatement("SELECT value FROM carray(?)");
        ps.setObject(1, arr);
        ResultSet rs = ps.executeQuery();

        List<Long> results = new ArrayList<>();
        while (rs.next())
            results.add(rs.getLong(1));

        assertEquals(2, results.size());
        assertEquals(Long.valueOf(42), results.get(0));
        assertEquals(Long.valueOf(43), results.get(1));
        rs.close();
        ps.close();
    }

    /* --- WHERE IN with carray --- */

    @Test
    public void testWhereInCarray() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute("DROP TABLE IF EXISTS t_carray_in");
        stmt.execute("CREATE TABLE t_carray_in (i INTEGER)");

        for (int i = 1; i <= 10; i++)
            stmt.execute("INSERT INTO t_carray_in VALUES(" + i + ")");

        Comdb2PreparedStatement ps = (Comdb2PreparedStatement) conn.prepareStatement(
                "SELECT i FROM t_carray_in WHERE i IN (SELECT value FROM carray(?)) ORDER BY i");
        ps.setInt32Array(1, new int[]{2, 5, 8});
        ResultSet rs = ps.executeQuery();

        List<Integer> results = new ArrayList<>();
        while (rs.next())
            results.add(rs.getInt(1));

        assertEquals(3, results.size());
        assertEquals(Integer.valueOf(2), results.get(0));
        assertEquals(Integer.valueOf(5), results.get(1));
        assertEquals(Integer.valueOf(8), results.get(2));

        rs.close();
        ps.close();
        stmt.execute("DROP TABLE t_carray_in");
        stmt.close();
    }

    /* --- Edge cases --- */

    @Test
    public void testSingleElementArray() throws SQLException {
        Comdb2PreparedStatement ps = (Comdb2PreparedStatement)
                conn.prepareStatement("SELECT value FROM carray(?)");
        ps.setInt64Array(1, new long[]{999L});
        ResultSet rs = ps.executeQuery();

        assertTrue(rs.next());
        assertEquals(999L, rs.getLong(1));
        assertFalse(rs.next());
        rs.close();
        ps.close();
    }

    @Test
    public void testClearAndRebind() throws SQLException {
        Comdb2PreparedStatement ps = (Comdb2PreparedStatement)
                conn.prepareStatement("SELECT value FROM carray(?)");
        ps.setInt32Array(1, new int[]{1, 2});
        ResultSet rs = ps.executeQuery();
        int count = 0;
        while (rs.next()) count++;
        assertEquals(2, count);
        rs.close();

        ps.clearParameters();

        ps.setInt32Array(1, new int[]{10, 20, 30, 40});
        rs = ps.executeQuery();
        count = 0;
        while (rs.next()) count++;
        assertEquals(4, count);
        rs.close();
        ps.close();
    }

    /* --- Validation tests --- */

    @Test(expected = SQLException.class)
    public void testEmptyArrayRejected() throws SQLException {
        conn.createArrayOf("int32", new Object[]{});
    }

    @Test(expected = SQLException.class)
    public void testNullArrayRejected() throws SQLException {
        conn.createArrayOf("int32", null);
    }

    @Test(expected = SQLException.class)
    public void testInvalidTypeRejected() throws SQLException {
        conn.createArrayOf("varchar", new Object[]{1, 2});
    }

    @Test
    public void testMaxElementsArray() throws SQLException {
        int size = Constants.CDB2_MAX_BIND_ARRAY;
        Object[] arr = new Object[size];
        for (int i = 0; i < size; i++)
            arr[i] = i;

        Array sqlArr = conn.createArrayOf("int32", arr);
        PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM carray(?)");
        ps.setArray(1, sqlArr);
        ResultSet rs = ps.executeQuery();

        assertTrue(rs.next());
        assertEquals(size, rs.getInt(1));
        rs.close();
        ps.close();
    }

    @Test(expected = SQLException.class)
    public void testExceedMaxElementsRejected() throws SQLException {
        Object[] tooMany = new Object[Constants.CDB2_MAX_BIND_ARRAY + 1];
        for (int i = 0; i < tooMany.length; i++)
            tooMany[i] = i;
        conn.createArrayOf("int32", tooMany);
    }
}
/* vim: set sw=4 ts=4 et: */
