using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LcLib.DALLib
{
    /// <summary>
    /// 表别名属性
    /// </summary>
    [AttributeUsage(System.AttributeTargets.Class, Inherited = true, AllowMultiple = true)]
    public sealed class TableAttribute : System.Attribute
    {
        /// <summary>
        /// 表别名
        /// </summary>
        public string TableName { get; set; }
    }

    /// <summary>
    /// 列属性
    /// </summary>
    [AttributeUsage(System.AttributeTargets.Property, Inherited = true, AllowMultiple = true)]
    public sealed class ColumnAttribute : System.Attribute
    {
        /// <summary>
        /// 是否标示
        /// </summary>
        public bool Identity { get; set; }
        /// <summary>
        /// 是否主键
        /// </summary>
        public bool PrimaryKey { get; set; }
        /// <summary>
        /// 列别名
        /// </summary>
        public string ColumnName { get; set; }
        /// <summary>
        /// Oracle序列名称
        /// </summary>
        public string SeqenceName { get; set; }
        /// <summary>
        /// 是否生成where
        /// </summary>
        public bool NotCreateWhere { get; set; }
    }
}
