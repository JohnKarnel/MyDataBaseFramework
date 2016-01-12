using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace MyDataBaseFramework
{
    public class ColumnData
    {
        private string _name;
        private SqlDbType _type;
        private int _typeLength;
        private bool _isPrimaryKey;
        private bool _isUnique;
        private bool _allowNull;
        private bool _identity;
        private ForeignKey _foreignKey;

        public ColumnData(string name, SqlDbType type, int typeLength = 100, bool allowNull = true, bool isPrimaryKey = false, bool identity = false, ForeignKey foreignKey = null, bool isUnique = false)
        {
            Name = name;
            Type = type;
            TypeLength = typeLength;
            IsPrimaryKey = isPrimaryKey;
            IsUnique = isUnique;
            AllowNull = allowNull;
            Identity = identity;
            ForeignKey = foreignKey;
        }
        public string Name
        {
            get { return _name; }
            set { _name = value; }
        }

        public SqlDbType Type
        {
            get { return _type; }
            set { _type = value; }
        }

        public int TypeLength
        {
            get { return _typeLength; }
            private set { _typeLength = value; }
        }

        public bool IsPrimaryKey
        {
            get { return _isPrimaryKey; }
            set { _isPrimaryKey = value; }
        }

        public bool IsUnique
        {
            get { return _isUnique; }
            set { _isUnique = value; }
        }

        public bool AllowNull
        {
            get { return _allowNull; }
            set { _allowNull = value; }
        }

        public bool Identity
        {
            get { return _identity; }
            set { _identity = value; }
        }

        public ForeignKey ForeignKey
        {
            get { return _foreignKey; }
            set { _foreignKey = value; }
        }
    }
}
