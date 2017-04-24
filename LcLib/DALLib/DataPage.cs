using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LcLib.DALLib
{
    public abstract class DataPage
    {
        /// <summary>
        /// 当前页
        /// </summary>
        public int CurrentPage { get; set; }
        /// <summary>
        /// 下一页页码
        /// </summary>
        public int? NextPage { get; set; }
        /// <summary>
        /// 总页数
        /// </summary>
        public int TotalPages { get; set; }
        /// <summary>
        /// 总条目数
        /// </summary>
        public int TotalItems { get; set; }
        /// <summary>
        /// 每页条目数
        /// </summary>
        public int PerPageItems { get; set; }
    }

    public class TablePage : DataPage
    {
        /// <summary>
        /// 查询结果
        /// </summary>
        public DataTable Items { get; set; }
    }
}
