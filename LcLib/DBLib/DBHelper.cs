using LcLib.LcFun;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace LcLib.DBLib
{
    public class DBHelper : IDisposable
    {

        private string _connStr = string.Empty;
        private string _providerName = string.Empty;
        private DbProviderFactory _factory;
        private IDbConnection _sharedConn;
        public DBType dbType = DBType.SqlServer;
        private string _paramPrefix = "@";
        IDbTransaction _tran = null;
        static Regex rxParams = new Regex(@"(?<!@)@\w+", RegexOptions.Compiled);
        static Regex rxParamsPrefix = new Regex(@"(?<!@)@\w+", RegexOptions.Compiled);
        private Stopwatch _timeWatch = new Stopwatch();

        public DBHelper(string connStrName = null)
        {
            var providerName = "System.Data.SqlClient";
            if (string.IsNullOrEmpty(connStrName))
                connStrName = ConfigurationManager.ConnectionStrings[0].Name;

            if (ConfigurationManager.ConnectionStrings[connStrName] != null)
            {
                if (!string.IsNullOrEmpty(ConfigurationManager.ConnectionStrings[connStrName].ProviderName))
                    providerName = ConfigurationManager.ConnectionStrings[connStrName].ProviderName;
            }
            else
            {
                throw new InvalidOperationException("未能找到指定名称的连接字符串！");
            }
            _connStr = ConfigurationManager.ConnectionStrings[connStrName].ConnectionString;
            _providerName = providerName;
            CommonCreate();
        }

        /// <summary>
        /// 执行超时时间
        /// </summary>
        public int CommandTimeout { get; set; }

        private string _lastSql;
        private object[] _lastSqlArgs;
        public string LastCommand
        {
            get { return FormatCommand(_lastSql, _lastSqlArgs); }
        }

        /// <summary>
        /// 创建链接类型
        /// </summary>
        private void CommonCreate()
        {
            if (_providerName != null)
                _factory = DbProviderFactories.GetFactory(_providerName);
            string dbtype = (_factory == null ? _sharedConn.GetType() : _factory.GetType()).Name;
            if (dbtype.Contains("Oracle")) dbType = DBType.Oracle;
            else if (dbtype.Contains("MySql")) dbType = DBType.MySql;
            else if (dbtype.Contains("System.Data.SqlClient")) dbType = DBType.SqlServer;
            else if (_providerName.IndexOf("Oracle", StringComparison.InvariantCultureIgnoreCase) >= 0) dbType = DBType.Oracle;
            else if (_providerName.IndexOf("MySql", StringComparison.InvariantCultureIgnoreCase) >= 0) dbType = DBType.MySql;

            if (dbType == DBType.MySql && _connStr != null && _connStr.IndexOf("Allow User Variables=true") >= 0)
                _paramPrefix = "?";
            if (dbType == DBType.Oracle)
                _paramPrefix = ":";

            try
            {
                _sharedConn = _factory.CreateConnection();
                _sharedConn.ConnectionString = _connStr;
                _sharedConn.Open();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(ex.Message);
            }
        }

        /// <summary>
        /// 创建数据库连接
        /// </summary>
        /// <param name="commType"></param>
        /// <param name="sql"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        public IDbCommand CreateCommand(CommandType commType, string sql, params object[] args)
        {
            bool isStoredProcedure = false;
            if (commType == CommandType.StoredProcedure)
                isStoredProcedure = true;
            if (!isStoredProcedure)
            {
                var new_args = new List<object>();
                sql = ProcessParams(sql, args, new_args);
                args = new_args.ToArray();
            }

            // 参数前缀是@还是:
            if (_paramPrefix != "@")
                sql = rxParamsPrefix.Replace(sql, m => _paramPrefix + m.Value.Substring(1));
            sql = sql.Replace("@@", "@");		   // <- double @@ escapes a single @

            // 创建command并且加载参数
            IDbCommand cmd = _sharedConn.CreateCommand();
            cmd.Connection = _sharedConn;
            cmd.CommandText = sql;
            if (_tran != null)
            {
                cmd.Transaction = _tran;
            }
            cmd.CommandType = commType;
            foreach (var item in args)
            {
                AddParam(cmd, item, _paramPrefix);
            }

            if (dbType == DBType.Oracle)
            {
                if (cmd.GetType().GetProperty("BindByName") != null)
                    cmd.GetType().GetProperty("BindByName").SetValue(cmd, true, null);
            }

            if (!String.IsNullOrEmpty(sql))
                DoPreExecute(cmd);

            return cmd;
        }

        /// <summary>
        /// 给DBCommand加载参数
        /// </summary>
        /// <param name="cmd"></param>
        /// <param name="item"></param>
        /// <param name="ParameterPrefix"></param>
        void AddParam(IDbCommand cmd, object item, string ParameterPrefix)
        {
            bool isStoredProcedure = false;
            if (cmd.CommandType == CommandType.StoredProcedure)
            {
                isStoredProcedure = true;
            }
            var idbParam = item as IDbDataParameter;
            if (idbParam != null)
            {
                if (!isStoredProcedure)
                    idbParam.ParameterName = string.Format("{0}{1}", ParameterPrefix, cmd.Parameters.Count);
                cmd.Parameters.Add(idbParam);
                return;
            }
            //执行语句中带参数
            if (!isStoredProcedure)
            {
                var p = cmd.CreateParameter();
                p.ParameterName = string.Format("{0}{1}", ParameterPrefix, cmd.Parameters.Count);
                if (item == null)
                {
                    p.Value = DBNull.Value;
                }
                else
                {
                    var t = item.GetType();
                    if (t.IsEnum)		// PostgreSQL .NET driver wont cast enum to int
                    {
                        p.Value = (int)item;
                    }
                    else if (t == typeof(Guid))
                    {
                        p.Value = item.ToString();
                        p.DbType = System.Data.DbType.String;
                        p.Size = 40;
                    }
                    else if (t == typeof(string))
                    {
                        p.Size = Math.Max((item as string).Length + 1, 4000);		// Help query plan caching by using common size
                        p.Value = item;
                    }
                    else
                    {
                        p.Value = item;
                    }
                }
                cmd.Parameters.Add(p);
            }
            else
            {
                //存储过程
                PropertyInfo[] pis = item.GetType().GetProperties();
                foreach (PropertyInfo pi in pis)
                {
                    var p = cmd.CreateParameter();
                    p.ParameterName = string.Format("{0}{1}", ParameterPrefix, pi.Name);
                    var value = pi.GetValue(item, null);
                    if (value == null)
                        p.Value = DBNull.Value;
                    else
                    {
                        if (pi.PropertyType.IsEnum)		// PostgreSQL .NET driver wont cast enum to int
                        {
                            p.Value = (int)value;
                        }
                        else if (pi.PropertyType == typeof(Guid))
                        {
                            p.Value = value.ToString();
                            p.DbType = System.Data.DbType.String;
                            p.Size = 40;
                        }
                        else if (pi.PropertyType == typeof(string))
                        {
                            p.Size = Math.Max((value as string).Length + 1, 4000);
                            p.Value = value;
                        }
                        else
                        {
                            p.Value = value;
                        }
                    }
                    cmd.Parameters.Add(p);
                }
            }
        }

        /// <summary>
        /// 设置超时时间，度量查询执行时间
        /// </summary>
        /// <param name="cmd"></param>
        void DoPreExecute(IDbCommand cmd)
        {
            // Setup command timeout
            if (CommandTimeout != 0)
            {
                cmd.CommandTimeout = CommandTimeout;
            }
            else if (System.Configuration.ConfigurationManager.AppSettings["CommandTimeout"] != null)
            {
                int commTimeout = 0;
                if (int.TryParse(System.Configuration.ConfigurationManager.AppSettings["CommandTimeout"], out commTimeout))
                {
                    cmd.CommandTimeout = commTimeout;
                }
            }

            // Call hook
            OnExecutingCommand(cmd);

            // Save it
            _lastSql = cmd.CommandText;
            _lastSqlArgs = (from IDataParameter parameter in cmd.Parameters select parameter.Value).ToArray();
        }

        /// <summary>
        /// 度量语句执行时间
        /// </summary>
        /// <param name="cmd"></param>
        private void OnExecutingCommand(IDbCommand cmd)
        {
            _timeWatch.Reset();
            _timeWatch.Start();
        }

        /// <summary>
        /// 执行结束，调试状态输出执行语句，耗时
        /// </summary>
        /// <param name="cmd"></param>
        private void OnExecutedCommand(IDbCommand cmd)
        {
            _timeWatch.Stop();
            System.Diagnostics.Debug.WriteLine(LastCommand);
        }

        
        /// <summary>
        /// 格式化调试输出
        /// </summary>
        /// <param name="cmd"></param>
        /// <returns></returns>
        public string FormatCommand(IDbCommand cmd)
        {
            return FormatCommand(cmd.CommandText, (from IDataParameter parameter in cmd.Parameters select parameter.Value).ToArray());
        }

        /// <summary>
        /// 格式化调试输出方法
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        public string FormatCommand(string sql, object[] args)
        {
            var sb = new StringBuilder();
            if (sql == null)
                return "";

            sb.AppendFormat("{0}", sql);
            if (args != null && args.Length > 0)
            {
                sb.Append("\n");
                for (int i = 0; i < args.Length; i++)
                {
                    if (args[i] != null)
                        sb.AppendFormat("\t -> {0}{1} [{2}] = \"{3}\"\n", _paramPrefix, i, args[i].GetType().Name, args[i]);
                }
                sb.Remove(sb.Length - 1, 1);
            }
            sb.AppendFormat("\n总耗时：{0} ms", _timeWatch.ElapsedMilliseconds.ToString("N0"));
            return sb.ToString();
        }

        /// <summary>
        /// 处理参数
        /// </summary>
        /// <param name="_sql"></param>
        /// <param name="args_src"></param>
        /// <param name="args_dest"></param>
        /// <returns></returns>
        private string ProcessParams(string _sql, object[] args_src, List<object> args_dest)
        {
            return rxParams.Replace(_sql, m =>
            {
                string param = m.Value.Substring(1);
                object arg_val;
                int paramIndex;
                if (int.TryParse(param, out paramIndex))
                {
                    //参数如果是数字
                    if (paramIndex < 0 || paramIndex >= args_src.Length)
                        throw new ArgumentOutOfRangeException(string.Format("Parameter '@{0}' specified but only {1} parameters supplied (in `{2}`)", paramIndex, args_src.Length, _sql));
                    arg_val = args_src[paramIndex];
                }
                else
                {
                    // 参数如果不是数字则在args_src中查找名称相同的项
                    bool found = false;
                    arg_val = null;
                    foreach (var o in args_src)
                    {
                        if (o is IDbDataParameter)
                        {
                            IDbDataParameter idbPara = o as IDbDataParameter;
                            if (idbPara.ParameterName.TrimStart('@') == param)
                            {
                                arg_val = idbPara.Value;
                                found = true;
                                break;
                            }
                        }
                        else
                        {
                            var pi = o.GetType().GetProperty(param);
                            if (pi != null)
                            {
                                arg_val = pi.GetValue(o, null);
                                found = true;
                                break;
                            }
                        }
                    }

                    if (!found)
                        throw new ArgumentException(string.Format("Parameter '@{0}' specified but none of the passed arguments have a property with this name (in '{1}')", param, _sql));
                }

                // 将参数全部转换成数字参数
                if ((arg_val as System.Collections.IEnumerable) != null &&
                    (arg_val as string) == null &&
                    (arg_val as byte[]) == null)
                {
                    var sb = new StringBuilder();
                    foreach (var i in arg_val as System.Collections.IEnumerable)
                    {
                        sb.Append((sb.Length == 0 ? "@" : ",@") + args_dest.Count.ToString());
                        args_dest.Add(i);
                    }
                    return sb.ToString();
                }
                else
                {
                    args_dest.Add(arg_val);
                    return "@" + (args_dest.Count - 1).ToString();
                }
            }
            );
        }

        /// <summary>
        /// 出现异常时
        /// </summary>
        /// <param name="ex"></param>
        private void OnException(Exception ex)
        {
            System.Diagnostics.Debug.WriteLine(ex.ToString());
            System.Diagnostics.Debug.WriteLine(LastCommand);
        }

        /// <summary>
        /// 开始事物
        /// </summary>
        public void BeginTransaction()
        {
            _tran = _sharedConn.BeginTransaction();
        }

        /// <summary>
        /// 提交事务
        /// </summary>
        public void CommitTransaction()
        {
            if (_tran != null)
            {
                _tran.Commit();
                this.DisposeTransaction();
            }
        }
        /// <summary>
        /// 回滚事务
        /// </summary>
        public void RollbackTransaction()
        {
            if (_tran != null)
            {
                _tran.Rollback();
                this.DisposeTransaction();
            }
        }
        /// <summary>
        /// 释放事务资源
        /// </summary>
        private void DisposeTransaction()
        {
            if (_tran != null)
            {
                _tran.Dispose();
                _tran = null;
                _sharedConn.Close();
            }
        }

        /// <summary>
        /// 执行语句，返回影响行数
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        public int ExecuteNonQuery(string sql, params object[] args)
        {
            try
            {
                if (_sharedConn.State == ConnectionState.Closed)
                    _sharedConn.Open();
                using (var cmd = CreateCommand(CommandType.Text, sql, args))
                {
                    var retv = cmd.ExecuteNonQuery();
                    OnExecutedCommand(cmd);
                    return retv;
                }

            }
            catch (Exception x)
            {
                OnException(x);
                throw;
            }
            finally
            {
                if (_tran == null)
                    _sharedConn.Close();
            }
        }

        /// <summary>
        /// 执行语句，返回对象
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        public object ExecuteScalar(string sql, params object[] args)
        {
            try
            {
                if (_sharedConn.State == ConnectionState.Closed)
                    _sharedConn.Open();
                using (var cmd = CreateCommand(CommandType.Text, sql, args))
                {
                    object val = cmd.ExecuteScalar();
                    OnExecutedCommand(cmd);
                    return val;
                }
            }
            catch (Exception x)
            {
                OnException(x);
                throw;
            }
            finally
            {
                if (_tran == null)
                    _sharedConn.Close();
            }
        }

        /// <summary>
        /// 执行语句，返回具体类型对象
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        public T ExecuteScalar<T>(string sql, params object[] args)
        {
            try
            {
                if (_sharedConn.State == ConnectionState.Closed)
                    _sharedConn.Open();
                using (var cmd = CreateCommand(CommandType.Text, sql, args))
                {
                    object val = cmd.ExecuteScalar();
                    OnExecutedCommand(cmd);
                    return ConvertFun.ConvertType<T>(val);
                }
            }
            catch (Exception x)
            {
                OnException(x);
                throw;
            }
            finally
            {
                if (_tran == null)
                    _sharedConn.Close();
            }
        }

        /// <summary>
        /// 执行语句，返回DataTable
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        public DataTable ExecuteDataTable(string sql, params object[] args)
        {
            try
            {
                if (_sharedConn.State == ConnectionState.Closed)
                    _sharedConn.Open();
                using (var cmd = CreateCommand(CommandType.Text, sql, args))
                {
                    DbDataAdapter da = _factory.CreateDataAdapter();
                    da.SelectCommand = (DbCommand)cmd;
                    DataTable dt = new DataTable();
                    da.Fill(dt);
                    OnExecutedCommand(cmd);
                    return dt;
                }

            }
            catch (Exception x)
            {
                OnException(x);
                throw;
            }
            finally
            {
                if (_tran == null)
                    _sharedConn.Close();
            }
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}
