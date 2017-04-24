using LcLib.DBLib;
using LcLib.LcFun;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace LcLib.DALLib
{
    public class DALHelper
    {
        /// <summary>
        /// 根据语句，返回list
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        public IEnumerable<T> Query<T>(string sql, params object[] args) where T : class
        {
            using (var db = new LcLib.DBLib.DBHelper())
            {
                return ConvertFun.ConvertToIEnumerable<T>(db.ExecuteDataTable(sql, args));
            }
        }

        /// <summary>
        /// 根据语句，返回对象
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        public T QueryModel<T>(string sql, params object[] args) where T : class
        {
            using (var db = new LcLib.DBLib.DBHelper())
            {
                DataTable dt = db.ExecuteDataTable(sql, args);
                if (dt.Rows.Count > 0)
                    return ConvertFun.ConvertToModel<T>(dt.Rows[0]);
                else
                    return default(T);
            }
        }

        /// <summary>
        /// 对单表进行Insert操作
        /// </summary>
        /// <typeparam name="T">泛型T</typeparam>
        /// <param name="model">实体参数</param>
        /// <param name="tran">事务</param>
        /// <returns>返回影响的行数</returns>
        public int Insert<T>(T model) where T : class
        {
            string sql = string.Empty;
            string tableName = model.GetType().Name;

            object[] customAttribute = null;
            object[] tableAttribute = null;

            Type t = model.GetType();
            tableAttribute = t.GetCustomAttributes(typeof(TableAttribute), false);
            if (tableAttribute != null && tableAttribute.Length > 0)
                tableName = (tableAttribute[0] as TableAttribute).TableName;

            sql = "insert into " + tableName;
            StringBuilder fieldList = new StringBuilder();
            StringBuilder valueList = new StringBuilder();
            PropertyInfo[] pis = t.GetProperties();
            foreach (PropertyInfo pi in pis)
            {
                if (pi.GetValue(model, null) == null)//如果属性值为null 不插入
                {
                    continue;
                }

                customAttribute = pi.GetCustomAttributes(typeof(ColumnAttribute), false);
                if (customAttribute.Count() == 1 && (customAttribute[0] as ColumnAttribute).NotCreateWhere)
                    continue;
                if (customAttribute.Count() == 0 || (customAttribute.Count() == 1 && !(customAttribute[0] as ColumnAttribute).Identity))
                {
                    fieldList.AppendFormat(",{0}", pi.Name);
                    valueList.AppendFormat(",@{0}", pi.Name);
                }
            }
            if (fieldList.Length == 0 || valueList.Length == 0)
                throw new ArgumentException();

            sql = string.Format("insert into {0}({1}) values({2})", tableName, fieldList.Remove(0, 1), valueList.Remove(0, 1));

            using (var db = new LcLib.DBLib.DBHelper())
            {
                return db.ExecuteNonQuery(sql, model);
            }
        }

        /// <summary>
        /// 对单表进行Update操作
        /// </summary>
        /// <typeparam name="T">泛型T</typeparam>
        /// <param name="model">实体参数</param>
        /// <param name="tran">事务</param>
        /// <returns>返回影响的行数</returns>
        public int Update<T>(T model) where T : class
        {
            string sql = string.Empty;
            string tableName = model.GetType().Name;
            Type t = model.GetType();
            object[] customAttribute = null;
            object[] tableAttribute = null;

            tableAttribute = t.GetCustomAttributes(typeof(TableAttribute), false);
            if (tableAttribute != null && tableAttribute.Length > 0)
                tableName = (tableAttribute[0] as TableAttribute).TableName;

            StringBuilder keyValuePaire = new StringBuilder();
            StringBuilder condition = new StringBuilder();
            PropertyInfo[] pis = t.GetProperties();
            foreach (PropertyInfo pi in pis)
            {
                if (pi.GetValue(model, null) == null)//如果属性值为null 不更新
                {
                    continue;
                }
                customAttribute = pi.GetCustomAttributes(typeof(ColumnAttribute), false);
                if (customAttribute.Count() == 1 && (customAttribute[0] as ColumnAttribute).NotCreateWhere)
                    continue;
                if (customAttribute.Count() == 1 && (customAttribute[0] as ColumnAttribute).PrimaryKey)
                    condition.AppendFormat(" and {0}=@{0}", pi.Name);
                else
                    keyValuePaire.AppendFormat(",{0}=@{0}", pi.Name);
            }
            if (condition.Length == 0 || keyValuePaire.Length == 0)
                throw new ArgumentException();

            sql = string.Format("update {0} set {1} where 1=1 {2}", tableName, keyValuePaire.Remove(0, 1), condition);

            using (var db = new LcLib.DBLib.DBHelper())
            {
                return db.ExecuteNonQuery(sql, model);
            }
        }

        /// <summary>
        /// 对单表进行Delete操作
        /// </summary>
        /// <typeparam name="T">泛型T</typeparam>
        /// <param name="model">实体参数</param>
        /// <param name="tran">事务</param>
        /// <returns>返回影响的行数</returns>
        public int Delete<T>(T model) where T : class
        {
            string sql = string.Empty;
            string tableName = model.GetType().Name;
            Type t = model.GetType();
            object[] tableAttribute = null;

            tableAttribute = t.GetCustomAttributes(typeof(TableAttribute), false);
            if (tableAttribute != null && tableAttribute.Length > 0)
                tableName = (tableAttribute[0] as TableAttribute).TableName;

            StringBuilder condition = new StringBuilder();
            PropertyInfo[] pis = t.GetProperties();
            foreach (PropertyInfo pi in pis)
            {
                if (pi.GetValue(model, null) == null)//如果属性值为null 不更新
                {
                    continue;
                }
                object[] customAttribute = pi.GetCustomAttributes(typeof(ColumnAttribute), false);
                if (customAttribute.Count() == 1 && (customAttribute[0] as ColumnAttribute).NotCreateWhere)
                    continue;
                condition.AppendFormat(" and {0}=@{0}", pi.Name);
            }
            if (condition.Length == 0)
                throw new ArgumentException();

            sql = string.Format("delete from {0} where 1=1 {1}", tableName, condition);

            using (var db = new LcLib.DBLib.DBHelper())
            {
                return db.ExecuteNonQuery(sql, model);
            }
        }

        /// <summary>
        /// 根据对象获取查询条件，对象可以为实体或匿名对象
        /// </summary>
        /// <param name="db"></param>
        /// <param name="query"></param>
        /// <param name="createorder"></param>
        /// <returns></returns>
        public string GetWhereString(object query, bool createorder = true)
        {
            if (query == null)
                return string.Empty;
            StringBuilder where = new StringBuilder();
            StringBuilder keyWhere = new StringBuilder();
            string sortfields = string.Empty;
            string append = string.Empty;
            foreach (PropertyInfo pi in query.GetType().GetProperties())
            {
                object[] customAttribute = pi.GetCustomAttributes(typeof(ColumnAttribute), false);
                if (customAttribute.Count() == 1 && (customAttribute[0] as ColumnAttribute).NotCreateWhere)
                    continue;
                if (pi.GetValue(query, null) == null || string.IsNullOrEmpty(pi.GetValue(query, null).ToString().Trim()))
                    continue;
                //如果是追加的查询条件
                if (pi.Name.ToLower() == "append")
                {
                    append = pi.GetValue(query, null).ToString();
                }
                else if (pi.Name.ToLower() == "sortfields")
                {
                    sortfields = pi.GetValue(query, null).ToString();
                }
                else
                {
                    if (pi.Name.ToLower().EndsWith("_key"))
                        keyWhere.AppendFormat(" and {0} =@{1}", pi.Name.Substring(0, pi.Name.Length - 4), pi.Name);
                    else if (pi.Name.ToLower().Contains("areaid"))
                    {
                        if (pi.GetValue(query, null).ToString().EndsWith("0000"))
                            keyWhere.AppendFormat(" and {0} like '{1}%'", pi.Name, pi.GetValue(query, null).ToString().Substring(0, 2));
                        else if (pi.GetValue(query, null).ToString().EndsWith("00"))
                            keyWhere.AppendFormat(" and {0} like '{1}%'", pi.Name, pi.GetValue(query, null).ToString().Substring(0, 4));
                        else
                            keyWhere.AppendFormat(" and {0} =@{1}", pi.Name, pi.Name);
                    }
                    else if (pi.Name.ToLower().EndsWith("id"))
                        keyWhere.AppendFormat(" and {0} =@{1}", pi.Name, pi.Name);
                    else if (pi.Name.ToLower().EndsWith("_to"))
                        where.AppendFormat(" and {0} <=@{1}", pi.Name.Substring(0, pi.Name.Length - 3), pi.Name);
                    else if (pi.Name.ToLower().EndsWith("_from"))
                        where.AppendFormat(" and {0} >=@{1}", pi.Name.Substring(0, pi.Name.Length - 5), pi.Name);
                    else
                    {
                        if (pi.PropertyType.IsValueType || ((pi.PropertyType.IsGenericType && pi.PropertyType.GetGenericTypeDefinition().Equals(typeof(Nullable<>)))))
                            where.AppendFormat(" and {0} =@{0}", pi.Name);
                        else
                        {
                            if (pi.Name.ToLower().EndsWith("_lower"))
                            {
                                where.AppendFormat(" and lower({0}) like '%{1}%'", pi.Name.Substring(0, pi.Name.Length - 6), pi.GetValue(query, null).ToString());
                            }
                            else if (pi.Name.ToLower().EndsWith("_upper"))
                            {
                                where.AppendFormat(" and upper({0}) like '%{1}%'", pi.Name.Substring(0, pi.Name.Length - 6), pi.GetValue(query, null).ToString());
                            }
                            else
                            {
                                where.AppendFormat(" and {0} like '%{1}%'", pi.Name, pi.GetValue(query, null).ToString());
                            }
                        }
                    }
                }
            }
            if (!string.IsNullOrEmpty(append.Trim()))
                where.AppendFormat(" and {0}", append);
            if (!string.IsNullOrEmpty(sortfields) && createorder)
                where.AppendFormat(" order by {0}", sortfields);
            if (keyWhere.Length > 0)
                where.Insert(0, keyWhere.ToString());
            return where.ToString();
        }

        /// <summary>
        /// 分页查询，itemsPerPage=0时查询全部
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="itemsPerPage">每页条数</param>
        /// <param name="currentPage">当前页数</param>
        /// <param name="args">查询参数</param>
        /// <returns></returns>
        public TablePage QueryTablePage(string sql, int itemsPerPage = 20, int currentPage = 1, params object[] args)
        {
            using (var db = new LcLib.DBLib.DBHelper())
            {
                TablePage page = new TablePage() { CurrentPage = currentPage, PerPageItems = itemsPerPage };
                BuilderPageSql(sql, itemsPerPage, currentPage, db.dbType);
                if (itemsPerPage == 0)
                {
                    page.Items = db.ExecuteDataTable(_sqlPage, args);
                    page.TotalItems = page.Items.Rows.Count;
                    page.TotalPages = 1;
                }
                else
                {
                    int itemcount = db.ExecuteScalar<int>(_sqlCount, args);
                    page.TotalItems = itemcount;
                    page.TotalPages = (itemcount % itemsPerPage == 0 ? itemcount / itemsPerPage : itemcount / itemsPerPage + 1);
                    if (itemcount == 0)
                        page.Items = new DataTable();
                    else
                        page.Items = db.ExecuteDataTable(_sqlPage, args);
                }
                return page;
            }
        }

        private string _sqlPage;
        private string _sqlCount;

        Regex rxColumns = new Regex(@"\A\s*SELECT\s+((?:\((?>\((?<depth>)|\)(?<-depth>)|.?)*(?(depth)(?!))\)|.)*?)(?<!,\s+)\bFROM\b", RegexOptions.IgnoreCase | RegexOptions.Multiline | RegexOptions.Singleline | RegexOptions.Compiled);
        Regex rxOrderBy = new Regex(@"\bORDER\s+BY\s+(?:\((?>\((?<depth>)|\)(?<-depth>)|.?)*(?(depth)(?!))\)|[\w\(\)\.])+(?:\s+(?:ASC|DESC))?(?:\s*,\s*(?:\((?>\((?<depth>)|\)(?<-depth>)|.?)*(?(depth)(?!))\)|[\w\(\)\.])+(?:\s+(?:ASC|DESC))?)*", RegexOptions.IgnoreCase | RegexOptions.Multiline | RegexOptions.Singleline | RegexOptions.Compiled);
        Regex rxDistinct = new Regex(@"\ADISTINCT\s", RegexOptions.IgnoreCase | RegexOptions.Multiline | RegexOptions.Singleline | RegexOptions.Compiled);
        Regex rxGroupBy = new Regex(@"\bGROUP\s+BY\s", RegexOptions.IgnoreCase | RegexOptions.Multiline | RegexOptions.Singleline | RegexOptions.Compiled);

        /// <summary>
        /// 创建用于分页查询的sql语句
        /// </summary>
        private void BuilderPageSql(string _sql, int _pageSize, int _currentPage, DBLib.DBType dbType, bool noCount = false)
        {
            string sqlSelectRemoved, sqlOrderBy;
            if (!SplitSqlForPaging(_sql, out sqlSelectRemoved, out sqlOrderBy))
                throw new Exception("分页查询时不能解析用于查询的SQL语句");
            sqlSelectRemoved = rxOrderBy.Replace(sqlSelectRemoved, "");
            if (_pageSize != 0)
            {
                if (rxDistinct.IsMatch(sqlSelectRemoved) && dbType != DBType.MySql)
                {
                    sqlSelectRemoved = "hz_inner.* FROM (SELECT " + sqlSelectRemoved + ") hz_inner";
                }
                if (dbType == DBType.Oracle)
                {
                    _sqlPage = string.Format("select * from (select rownum rn,pages.* from (select {1} {0})pages where rownum<={3})hz_paged where rn > {2}", sqlOrderBy == null ? "ORDER BY  NULL" : sqlOrderBy, sqlSelectRemoved, (_currentPage - 1) * _pageSize, noCount ? (_currentPage * _pageSize) + 1 : _currentPage * _pageSize);
                }
                else if (dbType == DBType.MySql)
                {
                    _sqlPage = string.Format("SELECT {0} {1} LIMIT {2},{3}", sqlSelectRemoved, sqlOrderBy, (_currentPage - 1) * _pageSize, _pageSize + 1);
                }
                else
                {
                    _sqlPage = string.Format("SELECT * FROM (SELECT ROW_NUMBER() OVER ({0}) rn, {1}) hz_paged WHERE rn> {2} AND rn<= {3}", sqlOrderBy == null ? "ORDER BY (SELECT NULL)" : sqlOrderBy, sqlSelectRemoved, (_currentPage - 1) * _pageSize, noCount ? (_currentPage * _pageSize) + 1 : _currentPage * _pageSize);
                }
            }
            else
            {
                _sqlPage = string.Format("SELECT {0} {1}", sqlSelectRemoved, sqlOrderBy);
            }

        }

        /// <summary>
        /// 为分页查询对sql语句进行解析分割
        /// </summary>
        /// <param name="sqlSelectRemoved"></param>
        /// <param name="sqlOrderBy"></param>
        /// <returns></returns>
        private bool SplitSqlForPaging(string _sql, out string sqlSelectRemoved, out string sqlOrderBy)
        {
            sqlSelectRemoved = null;
            sqlOrderBy = null;

            var m = rxOrderBy.Match(_sql);
            Group g;
            if (!m.Success)
            {
                sqlOrderBy = null;
            }
            else
            {
                g = m.Groups[0];
                sqlOrderBy = g.ToString();
                _sql = _sql.Substring(0, g.Index) + _sql.Substring(g.Index + g.Length);
            }

            m = rxColumns.Match(_sql);
            if (!m.Success)
                return false;

            g = m.Groups[1];
            sqlSelectRemoved = _sql.Substring(g.Index);

            if (rxDistinct.IsMatch(sqlSelectRemoved))
            {
                _sqlCount = string.Format("select count(*) from ({0} {1} {2})A", _sql.Substring(0, g.Index), m.Groups[1].ToString().Trim(), _sql.Substring(g.Index + g.Length));
            }
            else if (rxGroupBy.IsMatch(sqlSelectRemoved))
            {
                _sqlCount = string.Format("select count(*) from ({0} {1} {2})A", _sql.Substring(0, g.Index), m.Groups[1].ToString().Trim(), _sql.Substring(g.Index + g.Length));

            }
            else
            {
                _sqlCount = _sql.Substring(0, g.Index) + "COUNT(*) " + _sql.Substring(g.Index + g.Length);
            }
            return true;
        }
    }
}
