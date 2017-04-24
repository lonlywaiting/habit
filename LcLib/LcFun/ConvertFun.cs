using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace LcLib.LcFun
{
    public class ConvertFun
    {
        /// <summary>
        /// 将对象转换为具体类型
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        /// <returns></returns>
        public static T ConvertType<T>(object value)
        {
            Type type = typeof(T);
            if (IsNullOrEmptyOrDBNull(value))
                value = type.IsValueType ? Activator.CreateInstance(type) : null;
            if (type.IsGenericType && type.GetGenericTypeDefinition().Equals(typeof(Nullable<>)))
            {
                try
                {
                    return (T)Convert.ChangeType(value, type.GetGenericArguments()[0]);
                }
                catch
                {
                    value = null;
                    return (T)value;
                }
            }
            if (type.IsEnum)
                return (T)Enum.ToObject(type, value);
            return (T)Convert.ChangeType(value, typeof(T));
        }

        /// <summary>
        /// 判断null或DBNull或空字符串
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static bool IsNullOrEmptyOrDBNull(object obj)
        {
            return ((obj is DBNull) || obj == null || string.IsNullOrEmpty(obj.ToString())) ? true : false;
        }

        /// <summary>
        /// 将DataTable转换为list
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table"></param>
        /// <returns></returns>
        public static IEnumerable<T> ConvertToIEnumerable<T>(DataTable table) where T : class
        {
            foreach (DataRow row in table.Rows)
            {
                yield return ConvertToModel<T>(row);
            }
        }

        /// <summary>
        /// 将DataRow转换为对象
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dRow"></param>
        /// <returns></returns>
        public static T ConvertToModel<T>(DataRow dRow) where T : class
        {
            try
            {
                List<string> drItems = new List<string>(dRow.ItemArray.Length);
                for (int i = 0; i < dRow.ItemArray.Length; i++)
                {
                    drItems.Add(dRow.Table.Columns[i].ColumnName.ToLower());
                }
                T model = Activator.CreateInstance<T>();
                foreach (PropertyInfo pi in model.GetType().GetProperties(BindingFlags.GetProperty | BindingFlags.Public | BindingFlags.Instance))
                {
                    if (drItems.Contains(pi.Name.ToLower()))
                    {
                        if (pi.PropertyType.IsEnum) //属性类型是否表示枚举
                        {
                            object enumName = Enum.ToObject(pi.PropertyType, pi.GetValue(model, null));
                            pi.SetValue(model, enumName, null); //获取枚举值，设置属性值
                        }
                        else
                        {
                            if (!IsNullOrEmptyOrDBNull(dRow[pi.Name]))
                            {
                                pi.SetValue(model, ConvertNullableType(dRow[pi.Name], pi.PropertyType), null);
                            }
                        }
                    }
                }
                return model;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// 转换对象类型
        /// </summary>
        /// <param name="value"></param>
        /// <param name="mType"></param>
        /// <returns></returns>
        public static object ConvertNullableType(object value, Type mType)
        {
            if (mType.IsGenericType && mType.GetGenericTypeDefinition().Equals(typeof(Nullable<>)))
            {
                if (IsNullOrEmptyOrDBNull(value))
                    return null;
                System.ComponentModel.NullableConverter nullableConverter = new System.ComponentModel.NullableConverter(mType);
                mType = nullableConverter.UnderlyingType;
            }
            if (mType == typeof(bool) || mType == typeof(Boolean))
            {
                if (value is string)
                {
                    if (value.ToString() == "1")
                        return true;
                    else
                        return false;
                }
            }
            if (mType.IsEnum) //属性类型是否表示枚举
            {
                int intvalue;
                if (int.TryParse(value.ToString(), out intvalue))
                    return Enum.ToObject(mType, Convert.ToInt32(value));
                else
                    return System.Enum.Parse(mType, value.ToString(), false);
            }
            return Convert.ChangeType(value, mType);
        }
    }
}
