using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MyDataBaseFramework
{
    public class ForeignKey
    {
        private string _columnName;
        private string _tableName;

        public ForeignKey(string column, string table)
        {
            ColumnName = column;
            TableName = table;
        }

        public string ColumnName
        {
            get { return _columnName; }
            private set { _columnName = value; }
        }
        
        public string TableName
        {
            get { return _tableName; }
            private set { _tableName = value; }
        }
    }
}
