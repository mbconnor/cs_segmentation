//////////// PEX CS Priority Scoring Change Log

// 10-01-2017 - MBC - Source given to DWH for running in production
// 10-03-2017 - MBC - Fixed issue with approxQuantile - cannot take splits array with 2 equal values
//                    Added some conditional logic to handle CN_N and CN_S data (no forum or CS data)

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.ml.evaluation._
import java.util.Calendar


object cs_priority_ranker {
  
  def main(args: Array[String]) {
  
    val conf = new SparkConf().setAppName("cs_priority_ranker")
    val sc = new SparkContext(conf)
    
    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"

    sc.setLogLevel("WARN")
    
    val spark = (
    SparkSession.builder()
    .appName("cs-ranker")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()
    )

    import spark.implicits._
    import spark.sql
    
    /*
    val now = Calendar.getInstance()
    val date = now.get(Calendar.MONTH).toInt+1 + "_" + (now.get(Calendar.DAY_OF_MONTH)).toString + "_" + now.get(Calendar.YEAR).toString
    */
    
    
    val realm_filter = "'" + args(0) + "'"
    
    val date = "'" + args(1) + "'"
    
    
    /*
    val realm_filter = "'CN_N'"
    val date = "'2017-10-01'"
    */
    
    /*table location*/
    
    val outdir = "dw_ds"
    val outtable = "f_platf_acc_cs_profile_dd"
    
    /*val day = spark.sql("""select max(day_id) as max_day_id from dw_ds.f_platf_acc_cs_profile_dd""")*/

    val df = spark.sql("""select * from dw_ds.f_platf_acc_cs_profile_dd""")
    
    val data = df.filter("realm_code =" + realm_filter).filter("day_id =" + date)

    val data_reduced = (
      data.select("spa_key",
                  "realm_code",
                  "day_id",
                  "days_since_last_purchase", 
                  "purchased_1d_amt",
                  "purchased_7d_amt",
                  "purchased_30d_amt",
                  "purchased_total_amt",
                  "days_since_last_login",
                  "avg_session_time_in_mins_7d",
                  "sessions_7d_cnt",
                  "battles_7d_cnt",
                  "titles_played_7d_cnt",
                  "sessions_30d_cnt",
                  "avg_session_time_in_mins_30d",
                  "titles_played_30d_cnt",
                  "battles_30d_cnt",
                  "post_total_cnt",
                  "num_referrals_total",
                  "prop_of_mplayer_sessions_30d",
                  "mplayer_battles_7d_cnt",
                  "tickets_7d",
                  "days_since_last_cs_interaction",
                  "num_of_cs_inter_total",
                  "rate_of_cs_inter_30d"
                  )
      withColumn("dt", 'day_id cast StringType)
      withColumn("days_since_last_purchase", 'days_since_last_purchase cast DoubleType)
      withColumn("purchased_1d_amt", 'purchased_1d_amt cast DoubleType)
      withColumn("purchased_7d_amt", 'purchased_7d_amt cast DoubleType)
      withColumn("purchased_30d_amt", 'purchased_30d_amt cast DoubleType)
      withColumn("purchased_total_amt", 'purchased_total_amt cast DoubleType)
      withColumn("days_since_last_login", 'days_since_last_login cast DoubleType)
      withColumn("sessions_7d_cnt", 'sessions_7d_cnt cast DoubleType)
      withColumn("battles_7d_cnt", 'battles_7d_cnt cast DoubleType)
      withColumn("titles_played_7d_cnt", 'titles_played_7d_cnt cast DoubleType)
      withColumn("sessions_30d_cnt", 'sessions_30d_cnt cast DoubleType)
      withColumn("avg_session_time_in_mins_30d", 'avg_session_time_in_mins_30d cast DoubleType)
      withColumn("titles_played_30d_cnt", 'titles_played_30d_cnt cast DoubleType)
      withColumn("battles_30d_cnt", 'battles_30d_cnt cast DoubleType)
      withColumn("post_total_cnt", 'post_total_cnt cast DoubleType)
      withColumn("num_referrals_total", 'num_referrals_total cast DoubleType)
      withColumn("prop_of_mplayer_sessions_30d", 'prop_of_mplayer_sessions_30d cast DoubleType)
      withColumn("mplayer_battles_7d_cnt", 'mplayer_battles_7d_cnt cast DoubleType)
      withColumn("tickets_7d", 'tickets_7d cast DoubleType)
      withColumn("days_since_last_cs_interaction", 'days_since_last_cs_interaction cast DoubleType)
      withColumn("num_of_cs_inter_total", 'num_of_cs_inter_total cast DoubleType)
      withColumn("rate_of_cs_inter_30d", 'rate_of_cs_inter_30d cast DoubleType)
    )
    
       
    val decile_array = Array(0.01, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.99)
    val quintile_array = Array(0.2, 0.4, 0.6, 0.8, 0.99)
    val class_splits = Array(0.25, 0.5, 0.75, 0.9, 0.99)
    
    val low_vals_minus = Array[Double](Double.NegativeInfinity,-1, 0)
    val low_vals_no_minus = Array[Double](Double.NegativeInfinity, 0)
    val high_vals = Array[Double](Double.PositiveInfinity)
    
    val scored_data = data_reduced.select("spa_key")
    
    def binData(data:DataFrame, low_val: Array[Double], splits_array: Array[Double], col_name: String, scoring_expression: Column, custom: Boolean = false): DataFrame = {
      
      var data_to_bin = data
      var ntile = data_to_bin.filter(col_name + " > 0").toDF.stat.approxQuantile(col_name, splits_array, 0.001)
      var splits = low_val ++ ntile ++ high_vals
      var bucketizer = new Bucketizer()
      bucketizer.clear(bucketizer.params(0)).clear(bucketizer.params(1)).clear(bucketizer.params(2)).clear(bucketizer.params(3))
      bucketizer.setInputCol(col_name).setOutputCol(col_name + "_bin").setSplits(splits.distinct)
      var data_reduced_bins = bucketizer.transform(data_to_bin)
      
      var express = scoring_expression
      
      if(!custom) {
          var length_splits = splits.length - 3
          var express = expr(col_name + "_bin" + "/" + length_splits.toString)
      }
           
      var data_reduced_bins_scored = data_reduced_bins.withColumn(col_name + "_points", express)
      data_reduced_bins_scored = data_reduced_bins_scored.drop(col_name)
      
      return data_reduced_bins_scored
    }
    
    val weights_monetization = Array("0.19", "0.24","0.19", "0.13", "0.25")
    val cols_monetization = Array("days_since_last_purchase_points", 
                                  "purchased_1d_amt_points", 
                                  "purchased_7d_amt_points", 
                                  "purchased_30d_amt_points", 
                                  "purchased_total_amt_points")
                                  
    val weights_play = Array("0.159", "0.167", "0.171", "0.127", "0.114", "0.147", "0.114")
    val cols_play = Array("days_since_last_login_points", 
                          "sessions_7d_cnt_points", 
                          "battles_7d_cnt_points", 
                          "titles_played_7d_cnt_points",
                          "sessions_30d_cnt_points",
                          "avg_session_time_in_mins_30d_points",
                          "titles_played_30d_cnt_points")
                          
    val weights_social = Array("0.29","0.24","0.25","0.22")
    val cols_social = Array("post_total_cnt_points",
                          "num_referrals_total_points",
                          "prop_of_mplayer_sessions_30d_points",
                          "mplayer_battles_7d_cnt_points")    

    val weights_cs = Array("0.18","0.22","0.30","0.31")
    val cols_cs = Array("tickets_7d_points",
                          "days_since_last_cs_interaction_points",
                          "num_of_cs_inter_total_points",
                          "rate_of_cs_inter_30d_points")
                          
    val weights_overall = Array("0.33","0.30","0.22","0.15")
    val cols_overall = Array("score_monetization",
                             "score_play",
                             "score_social",
                             "score_cs") 
                          
    def scoreMonetization(data: DataFrame, weights: Array[String], col_list: Array[String]): DataFrame = {
        
        var data_to_score = data
        var scored_data = data_to_score.withColumn("score_monetization", expr(
                                                                              col_list(0) + "*" + weights(0) + "+" +
                                                                              col_list(1) + "*" + weights(1) + "+" +
                                                                              col_list(2) + "*" + weights(2) + "+" +
                                                                              col_list(3) + "*" + weights(3) + "+" +
                                                                              col_list(4) + "*" + weights(4)))
        
        return scored_data
    }
    
    def scorePlay(data: DataFrame, weights: Array[String], col_list: Array[String]): DataFrame = {
        
        var data_to_score = data
        var scored_data = data_to_score.withColumn("score_play", expr(
                                                                              col_list(0) + "*" + weights(0) + "+" +
                                                                              col_list(1) + "*" + weights(1) + "+" +
                                                                              col_list(2) + "*" + weights(2) + "+" +
                                                                              col_list(3) + "*" + weights(3) + "+" +
                                                                              col_list(4) + "*" + weights(4) + "+" +
                                                                              col_list(5) + "*" + weights(5) + "+" +
                                                                              col_list(6) + "*" + weights(6)))
        
        return scored_data
    }
    
    def scoreSocial(data: DataFrame, weights: Array[String], col_list: Array[String]): DataFrame = {
        
        var data_to_score = data
        var scored_data = data_to_score.withColumn("score_social", expr(
                                                                              col_list(0) + "*" + weights(0) + "+" +
                                                                              col_list(1) + "*" + weights(1) + "+" +
                                                                              col_list(2) + "*" + weights(2) + "+" +
                                                                              col_list(3) + "*" + weights(3)))
        
        return scored_data
    }    
    
    def scoreCS(data: DataFrame, weights: Array[String], col_list: Array[String]): DataFrame = {
        
        var data_to_score = data
        var scored_data = data_to_score.withColumn("score_cs", expr(
                                                                              col_list(0) + "*" + weights(0) + "+" +
                                                                              col_list(1) + "*" + weights(1) + "+" +
                                                                              col_list(2) + "*" + weights(2) + "+" +
                                                                              col_list(3) + "*" + weights(3)))
        
        return scored_data
    }

    def scoreOverall(data: DataFrame, weights: Array[String], col_list: Array[String]): DataFrame = {
        
        var data_to_score = data
        var scored_data = data_to_score.withColumn("score_overall", expr(
                                                                              col_list(0) + "*" + weights(0) + "+" +
                                                                              col_list(1) + "*" + weights(1) + "+" +
                                                                              col_list(2) + "*" + weights(2) + "+" +
                                                                              col_list(3) + "*" + weights(3)))
        
        return scored_data
    }


    
    //////////////////////////////////////////
    //create bins for days_since_last_purchase
    //////////////////////////////////////////
    
    var express1 = expr("CASE WHEN 1/days_since_last_purchase_bin = 1 THEN 0 ELSE 1/days_since_last_purchase_bin*2 END")
                              
    val data_temp = binData(data_reduced, low_vals_minus, decile_array, "days_since_last_purchase", express1, custom = true)
    
      
    //////////////////////////////////
    //create bins for purchased_1d_amt
    //////////////////////////////////
    
    var express2 = expr("purchased_1d_amt_bin/12")
                              
    val data_temp2 = binData(data_temp, low_vals_no_minus, decile_array, "purchased_1d_amt", express2)
   
    
    //////////////////////////////////
    //create bins for purchased_7d_amt
    //////////////////////////////////
    
    val express3 = expr("purchased_7d_amt_bin/12")
                              
    val data_temp3 = binData(data_temp2, low_vals_no_minus, decile_array, "purchased_7d_amt", express3)
    
    //////////////////////////////////
    //create bins for purchased_30d_amt
    //////////////////////////////////
    
    val express4 = expr("purchased_30d_amt_bin/12")
                              
    val data_temp4 = binData(data_temp3, low_vals_no_minus, decile_array, "purchased_30d_amt", express4)
                                            
    //////////////////////////////////
    //create bins for purchased_total_amt
    //////////////////////////////////
    
    val express5 = expr("purchased_total_amt_bin/12")
                              
    val data_temp5 = binData(data_temp4, low_vals_no_minus, decile_array, "purchased_total_amt", express5)
 
    //////////////////////////////////
    //create bins for days_since_last_login
    //////////////////////////////////
    
    val express6 = expr("days_since_last_login_bin/7")
                              
    val data_temp6 = binData(data_temp5, low_vals_minus, quintile_array, "days_since_last_login", express6)
                                           
    //////////////////////////////////
    //create bins for sessions_7d_cnt
    //////////////////////////////////
    
    val express7 = expr("sessions_7d_cnt_bin/6")
                              
    val data_temp7 = binData(data_temp6, low_vals_no_minus, quintile_array, "sessions_7d_cnt", express7)

    //////////////////////////////////
    //create bins for battles_7d_cnt
    //////////////////////////////////

    val express8 = expr("battles_7d_cnt_bin/12")
                              
    val data_temp8 = binData(data_temp7, low_vals_no_minus, decile_array, "battles_7d_cnt", express8)
    
    //////////////////////////////////
    //create bins for titles_played_7d_cnt
    //////////////////////////////////
    
    var data_to_bin = data_temp8

    var custom_splits = Array[Double](Double.NegativeInfinity, 0, 1, 2, 3, 4, Double.PositiveInfinity)                                                                    
    
    var bucketizer = new Bucketizer().setInputCol("titles_played_7d_cnt").setOutputCol("titles_played_7d_cnt_bin").setSplits(custom_splits)
    var data_reduced_bins = bucketizer.transform(data_to_bin)
    
    var data_reduced_bins_scored = data_reduced_bins.withColumn("titles_played_7d_cnt_points", expr("titles_played_7d_cnt_bin/5"))
        
    val data_temp9 = data_reduced_bins_scored

    //////////////////////////////////
    //create bins for sessions_30d_cnt
    //////////////////////////////////
    
    val express10 = expr("sessions_30d_cnt_bin/6")
                              
    val data_temp10 = binData(data_temp9, low_vals_no_minus, quintile_array, "sessions_30d_cnt", express10)                                           
    
    //////////////////////////////////
    //create bins for avg_session_time_in_mins_30d
    //////////////////////////////////
    
    val express11 = expr("avg_session_time_in_mins_30d_bin/12")
                              
    val data_temp11 = binData(data_temp10, low_vals_no_minus, decile_array, "avg_session_time_in_mins_30d", express11)     
    
    //////////////////////////////////
    //create bins for titles_played_30d_cnt
    //////////////////////////////////

    val data_to_bin2 = data_temp11
   
    val bucketizer2 = new Bucketizer().setInputCol("titles_played_30d_cnt").setOutputCol("titles_played_30d_cnt_bin").setSplits(custom_splits)
    val data_reduced_bins2 = bucketizer2.transform(data_to_bin2)
    
    val data_reduced_bins_scored2 = data_reduced_bins2.withColumn("titles_played_30d_cnt_points", expr("(titles_played_30d_cnt_bin-1)/4"))
        
    val data_temp12 = data_reduced_bins_scored2
   
    //////////////////////////////////
    //create bins for battles_30d_cnt
    //////////////////////////////////
    
    val express13 = expr("battles_30d_cnt_bin/12")
                              
    val data_temp13 = binData(data_temp12, low_vals_no_minus, decile_array, "battles_30d_cnt", express13)  
                                            
    //////////////////////////////////
    //create bins for post_total_cnt
    //////////////////////////////////
   
    //SEA forums have lower post counts
      
      val express14 = if(realm_filter == "'SEA'") expr("post_total_cnt_bin/5") else expr("post_total_cnt_bin/6") 

    //Conditional logic for missing CN_N and CN_S data      
      val data_temp14 =  if(realm_filter == "'CN_N'" || realm_filter == "'CN_S'"){
                           data_temp13.withColumn("post_total_cnt_points", lit(0))
                           } else{
                               if(realm_filter == "'SEA'") binData(data_temp13, low_vals_no_minus, class_splits, "post_total_cnt", express14)
                                 else binData(data_temp13, low_vals_no_minus, quintile_array, "post_total_cnt", express14)
        }

    //////////////////////////////////
    //create bins for num_referrals_total
    //////////////////////////////////
        
    val data_to_bin3 = data_temp14
   
    val bucketizer3 = new Bucketizer().setInputCol("num_referrals_total").setOutputCol("num_referrals_total_bin").setSplits(custom_splits)
    val data_reduced_bins3 = bucketizer3.transform(data_to_bin3)
    
    val data_reduced_bins_scored3 = data_reduced_bins3.withColumn("num_referrals_total_points", expr("(num_referrals_total_bin)/5"))
        
    val data_temp15 = data_reduced_bins_scored3
    
    //////////////////////////////////
    //create bins for prop_of_mplayer_sessions_30d
    //////////////////////////////////
        
    val express16 = expr("prop_of_mplayer_sessions_30d_bin/12")
                              
    val data_temp16 = binData(data_temp15, low_vals_no_minus, decile_array, "prop_of_mplayer_sessions_30d", express16)
    
    //////////////////////////////////
    //create bins for mplayer_battles_7d_cnt
    //////////////////////////////////
    
    val express17 = expr("mplayer_battles_7d_cnt_bin/6")
                              
    val data_temp17 = binData(data_temp16, low_vals_no_minus, quintile_array, "mplayer_battles_7d_cnt", express17)
    
    //////////////////////////////////
    //create bins for tickets_7d
    //////////////////////////////////
    
    val data_to_bin4 = data_temp17
   
    val bucketizer4 = new Bucketizer().setInputCol("tickets_7d").setOutputCol("tickets_7d_bin").setSplits(custom_splits)
    val data_reduced_bins4 = bucketizer4.transform(data_to_bin4)
    
    val data_reduced_bins_scored4 = data_reduced_bins4.withColumn("tickets_7d_points", expr("(tickets_7d_bin)/5"))
        
    val data_temp18 = data_reduced_bins_scored4
    
    //////////////////////////////////
    //create bins for days_since_last_cs_interaction
    //////////////////////////////////
    
    val express19 = expr("days_since_last_cs_interaction_bin/12")
                              
    //val data_temp19 = binData(data_temp18, low_vals_no_minus, decile_array, "days_since_last_cs_interaction", express19)
    
    //Conditional logic for missing CN_N and CN_S data
    val data_temp19 =  if(realm_filter == "'CN_N'" || realm_filter == "'CN_S'"){
                         data_temp18.withColumn("days_since_last_cs_interaction_points", lit(0))
                           } else{
                               binData(data_temp18, low_vals_no_minus, decile_array, "days_since_last_cs_interaction", express19)
        }
    //////////////////////////////////
    //create bins for num_of_cs_inter_total
    //////////////////////////////////
    
    val express20 = expr("num_of_cs_inter_total_bin/6")
                              
    //val data_temp20 = binData(data_temp19, low_vals_no_minus, quintile_array, "num_of_cs_inter_total", express20)
    
    //Conditional logic for missing CN_N and CN_S data
    val data_temp20 =  if(realm_filter == "'CN_N'" || realm_filter == "'CN_S'"){
                         data_temp19.withColumn("num_of_cs_inter_total_points", lit(0))
                           } else{
                               binData(data_temp19, low_vals_no_minus, quintile_array, "num_of_cs_inter_total", express20)
        }    
    //////////////////////////////////
    //create bins for rate_of_cs_inter_30d
    //////////////////////////////////
    
    val express21 = expr("rate_of_cs_inter_30d_bin/5")
    
    val splits = Array(0.4, 0.6, 0.8, 0.99)
                              
    //val data_temp21 = binData(data_temp20, low_vals_no_minus, splits, "rate_of_cs_inter_30d", express21)

    //Conditional logic for missing CN_N and CN_S data    
    val data_temp21 =  if(realm_filter == "'CN_N'" || realm_filter == "'CN_S'"){
                         data_temp20.withColumn("rate_of_cs_inter_30d_points", lit(0))
                           } else{
                               binData(data_temp20, low_vals_no_minus, splits, "rate_of_cs_inter_30d", express21)
        } 

        
    val data_m_scored = scoreMonetization(data_temp21, weights_monetization, cols_monetization)
    val data_p_scored = scorePlay(data_m_scored, weights_play, cols_play)
    val data_s_scored = scoreSocial(data_p_scored, weights_social, cols_social)
    val data_cs_scored = scoreCS(data_s_scored, weights_cs, cols_cs)
    
    val data_o_scored = scoreOverall(data_cs_scored, weights_overall, cols_overall)
    
    
    var ntiles = data_o_scored.toDF.stat.approxQuantile("score_overall", class_splits, 0.0001)
    var split = Array(Double.NegativeInfinity) ++ ntiles ++ Array(Double.PositiveInfinity)
    var classer = new Bucketizer()
    classer.setInputCol("score_overall").setOutputCol("cs_class").setSplits(split)
    var data_classed = classer.transform(data_o_scored)
    
    var data_final = (data_classed.withColumn("priority_class", expr("""CASE WHEN cs_class = 5 THEN "Diamond"
                                                                     ELSE CASE WHEN cs_class = 4 THEN "Platinum"
                                                                     ELSE CASE WHEN cs_class = 3 THEN "Gold"
                                                                     ELSE CASE WHEN cs_class = 2 THEN "Silver"
                                                                     ELSE CASE WHEN cs_class = 1 THEN "Bronze"
                                                                     ELSE "No Status" END END END END END"""))
                                  
                                   
                     )
                                                          
    data_final.write.mode("overwrite").partitionBy("dt", "realm_code").saveAsTable(outdir + "." + outtable)
    
  }
}
