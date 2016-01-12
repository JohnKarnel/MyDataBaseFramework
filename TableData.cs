using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.Reflection;

namespace MyDataBaseFramework
{
    public class TableData
    {
        private string _name;
        private BaseData _dataBase;
        private List<ColumnData> _columns;
        private bool _isPhysical;

        public BaseData DataBase
        { 
            get { return _dataBase; }
            private set { _dataBase = value; }              
        }

        public string Name
        { 
            get { return _name; }
            private set { _name = value; }
        }

        public List<ColumnData> Columns
        {
            get { return _columns; }
            private set { _columns = value; }
        }

        public bool IsPhysical
        {
            get 
            {
                VerifyPhysicalModel();
                return _isPhysical; 
            }
            private set { _isPhysical = value; }
        }

        public List<string> ColumnNames
        {
            get
            {
                var result = new List<string>();
                foreach (var column in Columns)
                    result.Add(column.Name);
                return result;
            }
        }
        private TableData(string name, BaseData database, List<ColumnData> columns)
        {
            Name = name;
            DataBase = database;
            Columns = columns;
            VerifyPhysicalModel();
        }

        private void VerifyPhysicalModel()
        {
            SqlConnection connection;
            try
            {
                using (connection = BaseData.ConnectToDataBase(this.DataBase.Name, this.DataBase.DataSource))
                {
                    string SQLcommand = String.Format("SELECT name FROM sysobjects WHERE type = 'U'");
                    SqlCommand command = new SqlCommand(SQLcommand, connection);
                    using (SqlDataReader readerTableName = command.ExecuteReader())
                    {
                        while (readerTableName.Read())
                        {
                            if (readerTableName.GetSqlValue(0).ToString() == this.Name)
                            {
                                this.IsPhysical = true;
                                break;
                            }
                            else
                            {
                                this.IsPhysical = false;
                            }
                        }
                    }
                    string strCommandGetColumns = String.Format("SELECT * FROM SYSCOLUMNS WHERE ID = OBJECT_ID('{0}')", this.Name);
                    SqlCommand commandGetColumns = new SqlCommand(strCommandGetColumns, connection);
                    using (SqlDataReader readerColumns = commandGetColumns.ExecuteReader())
                    {
                        int i = 0;
                        while (readerColumns.Read())
                        {
                            if (readerColumns[0].ToString() != Columns[i].Name)
                            {
                                this.IsPhysical = false;
                                return;
                            }
                            i++;
                        }
                    }
                }
            }
            catch (Exception)
            {
                this.IsPhysical = false;
            }
        }

        public void CreatePhysicalModel()
        {
            if (IsPhysical)
                throw new Exception("This table is exist");
            if (Columns.Count > 0)
            {
                try
                {
                    using (SqlConnection connection = BaseData.ConnectToDataBase(this.DataBase.Name, this.DataBase.DataSource))
                    {
                        string SQLcommand = String.Format("CREATE TABLE [{0}] (",Name);
                        foreach (var column in Columns)
                        {
                            string name = column.Name;
                            string type = column.Type.ToString();
                            string typeLength;
                            if(column.Type == SqlDbType.Char || column.Type == SqlDbType.NChar || 
                               column.Type == SqlDbType.VarChar || column.Type == SqlDbType.NVarChar || 
                               column.Type == SqlDbType.Text || column.Type == SqlDbType.NText)
                            {
                                typeLength = String.Format("({0})", column.TypeLength.ToString());
                            }
                            else
                            {
                                typeLength = "";
                            }
                            string allowNull = column.AllowNull ? "NULL" : "NOT NULL";
                            string isPrimaryKey = column.IsPrimaryKey ? "PRIMARY KEY" : "";
                            string identity = column.Identity ? "IDENTITY" : "";
                            string isUnique = column.IsUnique ? "UNIQUE" : "";
                            string foreignKey = (column.ForeignKey != null) ? String.Format("foreign key references ([{0}]){1}", column.ForeignKey.TableName, column.ForeignKey.ColumnName) : "";
                            SQLcommand += String.Format("[{0}] {1} {2} {3} {4} {5} {6} {7},", name, type, typeLength, identity, isPrimaryKey, isUnique, allowNull, foreignKey);
                        }
                        SQLcommand = SQLcommand.Remove(SQLcommand.Length - 1) + ")";
                        SqlCommand command = new SqlCommand(SQLcommand, connection);
                        command.ExecuteNonQuery();
                        VerifyPhysicalModel();
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
            else
                throw new Exception(String.Format("Cannot create table {0} without columns", Name));
        }

        public static TableData CreateNew(string name, BaseData database, List<ColumnData> columns)
        {
            TableData table = new TableData(name, database, columns);
            return table;
        }

        public int Insert(List<string> values, List<string> columns = null)
        {
            int result = 0;
            columns = columns ?? this.ColumnNames;
            if(!this.IsPhysical)
                throw new Exception(String.Format("Your table {0} don`t exist on SQL Server", this.Name));
            string SQLcommand = String.Format("INSERT INTO [{0}] (", this.Name);
            foreach (var column in columns)
                SQLcommand += String.Format("{0},", column);
            SQLcommand = SQLcommand.Remove(SQLcommand.Length - 1) + ") VALUES (";
            foreach (var value in values)
                SQLcommand += String.Format("'{0}',", value);
            SQLcommand = SQLcommand.Remove(SQLcommand.Length - 1) + ")";
            try
            {
                using (SqlConnection connection = BaseData.ConnectToDataBase(this.DataBase.Name, this.DataBase.DataSource))
                {
                    SqlCommand command = new SqlCommand(SQLcommand, connection);
                    result = command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            return result;
        }

        public static int InsertInto(string tableName, string databaseName, List<string> values, List<string> columns = null)
        {
            BaseData bd = new BaseData(databaseName);
            TableData table = bd.GetTable(tableName);
            return table.Insert(values, columns);
        }

        public static DataTable SelectAll(string tableName, string databaseName)
        {
            DataTable result = new DataTable();
            using(SqlConnection connection = BaseData.ConnectToDataBase(databaseName))
            {
                string query = String.Format("SELECT * FROM [{0}]", tableName);
                SqlCommand command = new SqlCommand(query, connection);
                result.Load(command.ExecuteReader());
            }
            return result;
        }

        public static DataTable SelectProtection(string tableName, string databaseName, List<string> columnNames)
        {
            DataTable result = new DataTable();
            using (SqlConnection connection = BaseData.ConnectToDataBase(databaseName))
            {
                string query = "SELECT ";
                foreach(var column in columnNames)
                {
                    query += String.Format("{0},", column);
                }
                query = query.Remove(query.Length - 1) + String.Format(" FROM [{0}]", tableName);
                SqlCommand command = new SqlCommand(query, connection);
                result.Load(command.ExecuteReader());
            }
            return result;
        }

        public static DataTable SelectByRule(string tableName, string databaseName, List<string> columnNames, List<string> columnValues)
        {
            DataTable result = new DataTable();
            using (SqlConnection connection = BaseData.ConnectToDataBase(databaseName))
            {
                string query = String.Format("SELECT * FROM [{0}] WHERE ", tableName);
                for (int i = 0; i < columnNames.Count; i++)
                {
                    query += String.Format("{0}='{1}' and ", columnNames[i], columnValues[i]);
                }
                query = query.Remove(query.Length - 4);
                SqlCommand command = new SqlCommand(query, connection);
                SqlDataReader reader = command.ExecuteReader();
                result.Load(reader);
            }
            return result;
        }

        public int DeleteAll()
        {
            using (SqlConnection connection = BaseData.ConnectToDataBase(this.DataBase.Name))
            {
                string query = String.Format("DELETE [{0}]", this.Name);
                SqlCommand command = new SqlCommand(query, connection);
                return command.ExecuteNonQuery();
            }
        }

        public static int DeleteAll(string tableName, string databaseName)
        {
            BaseData bd = new BaseData(databaseName);
            TableData table = bd.GetTable(tableName);
            return table.DeleteAll();
        }

        public int DeleteByColumnValues(List<string> columns, List<string> values)
        {
            using (SqlConnection connection = BaseData.ConnectToDataBase(this.DataBase.Name))
            {
                string query = String.Format("DELETE [{0}] WHERE ", this.Name);
                for (int i = 0; i < columns.Count; i++)
                {
                    query += String.Format("{0}='{1}' and ", columns[i], values[i]);
                }
                query = query.Remove(query.Length - 4);
                SqlCommand command = new SqlCommand(query, connection);
                return command.ExecuteNonQuery();
            }
        }

        public static int DeleteByColumnValues(string tableName, string databaseName, List<string> columnNames, List<string> columnValues)
        {
            BaseData bd = new BaseData(databaseName);
            TableData table = bd.GetTable(tableName);
            return table.DeleteByColumnValues(columnNames, columnValues);
        }

        public int UpdateValue(string column, string oldValue, string newValue)
        {
            using (SqlConnection connection = BaseData.ConnectToDataBase(this.DataBase.Name))
            {
                string query = String.Format("UPDATE [{0}] SET {1}='{2}' WHERE {1} = '{3}'", this.Name, column, newValue, oldValue);
                SqlCommand command = new SqlCommand(query, connection);
                return command.ExecuteNonQuery();
            }
        }

        public static int Update(string tableName, string databaseName, List<string> columnNames, List<string> columnOldValues, List<string> columnNewValues)
        {
            int count = 0;
            BaseData bd = new BaseData(databaseName);
            TableData table = bd.GetTable(tableName);
            for(int i = 0; i < columnNames.Count; i++)
            {
               count += table.UpdateValue(columnNames[i], columnOldValues[i], columnNewValues[i]);
            }
            return count;
        }

        public static int UpdateWithRule(string tableName, string databaseName, List<string> columnNames, List<string> columnNewValues, List<string> ruleColumns, List<string> ruleValues)
        {
            using (SqlConnection connection = BaseData.ConnectToDataBase(databaseName))
            {
                string SQLcommand = String.Format("UPDATE [{0}] SET ", tableName);
                for (int i = 0; i < columnNames.Count; i++ )
                {
                    SQLcommand += String.Format("{0}='{1}',", columnNames[i], columnNewValues[i]);
                }
                SQLcommand = SQLcommand.Remove(SQLcommand.Length - 1) + " WHERE ";
                for (int i = 0; i < ruleColumns.Count; i++)
                {
                    SQLcommand += String.Format("{0}='{1}' and ", ruleColumns[i], ruleValues[i]);
                }
                SQLcommand = SQLcommand.Remove(SQLcommand.Length - 4);
                SqlCommand command = new SqlCommand(SQLcommand, connection);
                return command.ExecuteNonQuery();
            }
        }

        public void Drop()
        {
            using (SqlConnection connection = BaseData.ConnectToDataBase(this.DataBase.Name))
            {
                string query = String.Format("DROP TABLE [{0}]", this.Name);
                SqlCommand command = new SqlCommand(query, connection);
                command.ExecuteNonQuery();
            }
        }
    }
}
