using Cassandra;
using PolyPersist.Net.Common;
using System.Collections;
using System.Linq.Expressions;
using System.Reflection;

namespace PolyPersist.Net.ColumnStore.Cassandra.Linq
{
    internal class Cassandra_QueryProvider<TRow> : IQueryProvider
        where TRow : IRow, new()
    {
        private readonly Cassandra_ColumnTable<TRow> _table;

        public Cassandra_QueryProvider(Cassandra_ColumnTable<TRow> table)
        {
            _table = table;
        }

        public IQueryable CreateQuery(Expression expression)
        {
            var elementType = expression.Type.GetGenericArguments().First();
            var queryableType = typeof(Cassandra_Queryable<,>).MakeGenericType(typeof(TRow), elementType);
            return (IQueryable)Activator.CreateInstance(queryableType, this, expression);
        }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            return new Cassandra_Queryable<TRow, TElement>(this, expression);
        }

        private Cassandra_Query BuildQuery(Expression expression)
        {
            var visitor = new Cassandra_ExpressionVisitor(_table._session.Keyspace, _table._tableName, _table._tableMeta);
            visitor.Visit(expression);
            var (selectClause, resultType) = visitor.TranslateSelect();
            string distinctClause = visitor.TranslateDistinct();
            string whereClause = visitor.TranslateWhere();
            string orderByClause = visitor.TranslateOrderBy();
            string limitClause = visitor.TranslateLimit();
            string allowFiltering = visitor.IsAllowFiltering()
                ? " ALLOW FILTERING "
                : string.Empty;

            var cql = $"SELECT {distinctClause} {selectClause} FROM {_table._tableName}{whereClause}{allowFiltering}{orderByClause}{limitClause};";

            return new Cassandra_Query()
            {
                cql = cql,
                resultType = resultType,
                projectionCtorForAnonymous = visitor._projectionAnonymousCtor,
                projectionMemberMap = visitor._projectionMap,
            };
        }

        public object Execute(Expression expression)
        {
            var query = BuildQuery(expression);
            RowSet rs = _table._session.Execute(new SimpleStatement(query.cql));

            if (expression.Type == typeof(int))
            {
                if (query.resultType != Cassandra_Query.ResultTypes.Count)
                    throw new NotSupportedException("Only count is supported when expected result is integer");

                return (int)rs.First().GetValue<long>("count");
            }
            else if (expression.Type == typeof(bool))
            {
            }
            else if (typeof(IEnumerable).IsAssignableFrom(expression.Type))
            {
                switch (query.resultType)
                {
                    case Cassandra_Query.ResultTypes.SelectRow:
                        {
                            var accessors = MetadataHelper.GetAccessors<TRow>().ToDictionary(kvp => kvp.Key.ToLower(), kvp => kvp.Value);

                            //  build index map, for reaching values in row based on index instad of name
                            var columns = rs.Columns;
                            var fieldIndexMap = new Dictionary<string, int>();
                            for (int i = 0; i < columns.Length; i++)
                                fieldIndexMap[columns[i].Name.ToLower()] = i;

                            var mappedAccessors = accessors
                                .Select(kvp => (
                                    FieldIndex: fieldIndexMap[kvp.Key],
                                    MemberAccessor: kvp.Value,
                                    CassandraValueGetter: Cassandra_Mapper.BuildTypedGetter(kvp.Value.Type)))
                                .ToList();

                            IEnumerable<TRow> StreamRows()
                            {
                                foreach (var row in rs)
                                {
                                    var item = new TRow();

                                    foreach (var (fieldIndex, memberAccessor, cassandraValueGetter) in mappedAccessors)
                                    {
                                        if (memberAccessor.Setter != null)
                                        {
                                            var value = cassandraValueGetter(row, fieldIndex); ;
                                            memberAccessor.Setter(item, value);
                                        }
                                    }

                                    yield return item;
                                }
                            }

                            return StreamRows();
                        }
                    case Cassandra_Query.ResultTypes.ProjectionToClass:
                        {
                            Type resultItemType = expression.Type.GetGenericArguments().First();
                            var properties = resultItemType
                                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                                .Where(p => p.CanWrite)
                                .ToDictionary(p => p.Name.ToLower(), p => p);

                            var columns = rs.Columns;
                            var fieldIndexMap = new Dictionary<string, int>();
                            for (int i = 0; i < columns.Length; i++)
                                fieldIndexMap[columns[i].Name.ToLower()] = i;

                            var mappers = properties
                                .Select(kvp => (
                                    FieldIndex: fieldIndexMap[query.projectionMemberMap[kvp.Key]],
                                    Property: kvp.Value,
                                    CassandraValueGetter: Cassandra_Mapper.BuildTypedGetter(kvp.Value.PropertyType)))
                                .ToList();

                            IEnumerable<object> StreamRows()
                            {
                                foreach (var row in rs)
                                {
                                    var item = Activator.CreateInstance(resultItemType);

                                    foreach (var (fieldIndex, property, cassandraGetter) in mappers)
                                    {
                                        var value = cassandraGetter(row, fieldIndex);
                                        property.SetValue(item, value);
                                    }

                                    yield return item;
                                }
                            }

                            // reflection-al hívjuk meg Enumerable.Cast<T>()
                            var castMethod = typeof(Enumerable)
                                .GetMethod(nameof(Enumerable.Cast), BindingFlags.Static | BindingFlags.Public)
                                .MakeGenericMethod(resultItemType);

                            var castedEnumerable = castMethod.Invoke(null, new object[] { StreamRows() });

                            return castedEnumerable;
                        }
                    case Cassandra_Query.ResultTypes.ProjectionToAnonymous:
                        {
                            var columns = rs.Columns;
                            var fieldIndexMap = new Dictionary<string, int>();
                            for (int i = 0; i < columns.Length; i++)
                                fieldIndexMap[columns[i].Name.ToLower()] = i;

                            var mappers = query.projectionCtorForAnonymous
                                .Constructor
                                .GetParameters()
                                .Select((parameter, index) => (
                                    FieldIndex: fieldIndexMap[query.projectionMemberMap[parameter.Name]],
                                    Parameter: parameter,
                                    ParameterIndex: index,
                                    CassandraValueGetter: Cassandra_Mapper.BuildTypedGetter(parameter.ParameterType)))
                                .ToList();

                            IEnumerable<object> StreamRows()
                            {
                                foreach (var row in rs)
                                {
                                    var args = new object[mappers.Count];

                                    foreach (var (fieldIndex, param, paramIndex, cassandraGetter) in mappers)
                                    {
                                        var value = cassandraGetter(row, fieldIndex);
                                        args[paramIndex] = value;
                                    }

                                    yield return query.projectionCtorForAnonymous.Constructor.Invoke(args);
                                }
                            }

                            // reflection-al hívjuk meg Enumerable.Cast<T>()
                            Type resultItemType = expression.Type.GetGenericArguments().First();
                            var castMethod = typeof(Enumerable)
                                .GetMethod(nameof(Enumerable.Cast), BindingFlags.Static | BindingFlags.Public)
                                .MakeGenericMethod(resultItemType);

                            var castedEnumerable = castMethod.Invoke(null, new object[] { StreamRows() });

                            return castedEnumerable;
                        }
                    default:
                        throw new NotSupportedException("Only select row, or projection is alllowed with IEnumerable result");
                }
            }
            else
            {
            }

            return null;
        }
        public TResult Execute<TResult>(Expression expression) => (TResult)Execute(expression);

        //public async IAsyncEnumerable ExecuteAsync(Expression expression)
        //{
        //    var (cql, mapper) = BuildQueryPlan(expression);
        //    var statement = new SimpleStatement(cql).SetPageSize(5000);
        //    RowSet rs = await _table._session.ExecuteAsync(statement);

        //    do
        //    {
        //        foreach (var row in rs)
        //            yield return mapper(row, rs.Columns);

        //        if (rs.PagingState != null)
        //        {
        //            var pagedStatement = new SimpleStatement(cql)
        //                .SetPageSize(5000)
        //                .SetPagingState(rs.PagingState);
        //            rs = await _table._session.ExecuteAsync(pagedStatement);
        //        }
        //        else
        //        {
        //            break;
        //        }

        //    } while (true);
        //}
    }
}
