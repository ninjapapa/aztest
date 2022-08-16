import pyspark.sql.functions as F
import pyspark.sql.types as T

class EDD(object):
    """
    """
    debug = True

    def __init__(self):
        self.str_empty_null_ignore = True

    def _supported_types(self, dt):
        """
        """
        tmap = {
            T.StringType(): 'str_query_type',
            T.IntegerType(): 'num_query_type',
        }

        return tmap.get(dt)

    def _supported_queries(self, col_name, q_type):
        assert(q_type in ['str_query_type', 'num_query_type'])
        if (q_type == 'str_query_type'):
            return [
                F.count(
                    F.when(F.col(col_name).isNotNull() & (F.col(col_name) != ""), F.col(col_name))
                ).alias("{}_str_non_null_cnt".format(col_name)),
                F.min(F.length(col_name)).alias("{}_str_len_min".format(col_name)),
                F.max(F.length(col_name)).alias("{}_str_len_max".format(col_name)),
                F.avg(F.length(col_name)).alias("{}_str_len_avg".format(col_name)),
                F.sum(F.xxhash64(col_name)).alias("{}_str_hash_sum".format(col_name)),
            ]
        elif (q_type == 'num_query_type'):
            return [
                F.min(col_name).alias("{}_num_min".format(col_name)),
                F.max(col_name).alias("{}_num_max".format(col_name)),
                F.avg(col_name).alias("{}_num_avg".format(col_name)),
                F.sum(col_name).alias("{}_num_sum".format(col_name)),
                F.count(col_name).alias("{}_num_non_null_cnt".format(col_name)),       # Spark count is non-null count
            ]
        
    def _query_by_col(self, col_name, col_type):
        query_type = self._supported_types(col_type)
        return self._supported_queries(col_name, query_type)

    def run_query(self, df, group_by_key=None):
        """Run meta-data-based queries

        Args:
            df (DataFrame): input data to run query on
            group_by_key (str): column to group by for all aggregation queries. Default: None. None means aggregate on whole data

        Return:
            (DataFrame, Boolean): DataFrame of query result and top-level pass/fail

        Notes:
            To limit the range data to run query on, please pre-filter the DataFrame before passing in
        """

        # Get all column names and types:
        cols = [(c, df.schema[c].dataType) for c in df.columns if c != group_by_key]
        # flatten all queries in a 1-d list
        queries = [q for (c, t) in cols for q in self._query_by_col(c, t)]

        if (group_by_key is None):
            return df.agg(*queries)
        else:
            return df.groupBy(group_by_key).agg(*queries)



class UnmatchedRecs(object):
    """For a set of focused columns, look for un-matched records

    Args:
        df1 (DataFrame): left data
        df2 (DataFrame): right data
        pk (list(str)): Primary Key
        cols (list(str)): focused columns - default: None, all columns other than PK will be checked
    """
    def __init__(self, df1, df2, pk, cols=None):
        # make sure PK is a sub-set of cols
        assert(set(pk).issubset(set(df1.columns)))
        assert(set(pk).issubset(set(df2.columns)))

        non_pk_cols1 = set(df1.columns) - set(pk)
        non_pk_cols2 = set(df2.columns) - set(pk)
        non_pk_overlap = list(non_pk_cols1.intersection(non_pk_cols2))
        focused = cols if cols is not None else non_pk_overlap

        # rename overlapped cols
        df1_rn = df1
        df2_rn = df2
        for c in non_pk_overlap:
            df1_rn = df1_rn.withColumnRenamed(c, c + "_left")
            df2_rn = df2_rn.withColumnRenamed(c, c + "_right")

        # only check the overlapped records, assume aligned
        jdf = df1_rn.join(df2_rn, pk, 'inner')

        # build the not match condition
        not_match_cndtn = (F.col(focused[0] + '_left') != F.col(focused[0] + '_right'))
        for c in focused[1:]:
            not_match_cndtn = not_match_cndtn | (F.col(c + '_left') != F.col(c + '_right'))
        
        self.pk = pk
        self.focused = focused
        self.resultDF = jdf.where(not_match_cndtn)
        self.resultDF.cache()

    def only_unmatched_cols(self):
        cols = [c + postfix for c in self.focused for postfix in ["_left", "_right"]]
        return self.resultDF.select(*(self.pk), *cols)

