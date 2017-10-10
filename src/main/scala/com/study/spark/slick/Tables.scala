package com.study.spark.slick

// AUTO-GENERATED Slick data model
/** Stand-alone Slick data model for immediate use */
object Tables extends {
  val profile = slick.jdbc.MySQLProfile
} with Tables

/** Slick data model trait for extension, choice of backend or usage in the cake pattern. (Make sure to initialize this late.) */
trait Tables {
  val profile: slick.jdbc.JdbcProfile
  import profile.api._
  import slick.model.ForeignKeyAction
  // NOTE: GetResult mappers for plain SQL are only generated for tables where Slick knows how to map the types of all columns.
  import slick.jdbc.{GetResult => GR}

  /** DDL for all tables. Call .create to execute. */
  lazy val schema: profile.SchemaDescription = SessionAggrStat.schema ++ SessionDetail.schema ++ SessionRandomExtract.schema ++ Top10Category.schema ++ Top10CategorySession.schema
  @deprecated("Use .schema instead of .ddl", "3.0")
  def ddl = schema

  /** Entity class storing rows of table SessionAggrStat
   *  @param id Database column id SqlType(INT), PrimaryKey
   *  @param sessionCount Database column session_count SqlType(INT), Default(None)
   *  @param `1s3s` Database column 1s_3s SqlType(DOUBLE), Default(None)
   *  @param `4s6s` Database column 4s_6s SqlType(DOUBLE), Default(None)
   *  @param `7s9s` Database column 7s_9s SqlType(DOUBLE), Default(None)
   *  @param `10s30s` Database column 10s_30s SqlType(DOUBLE), Default(None)
   *  @param `30s60s` Database column 30s_60s SqlType(DOUBLE), Default(None)
   *  @param `1m3m` Database column 1m_3m SqlType(DOUBLE), Default(None)
   *  @param `3m10m` Database column 3m_10m SqlType(DOUBLE), Default(None)
   *  @param `10m30m` Database column 10m_30m SqlType(DOUBLE), Default(None)
   *  @param `30m` Database column 30m SqlType(DOUBLE), Default(None)
   *  @param `13` Database column 1_3 SqlType(DOUBLE), Default(None)
   *  @param `46` Database column 4_6 SqlType(DOUBLE), Default(None)
   *  @param `79` Database column 7_9 SqlType(DOUBLE), Default(None)
   *  @param `1030` Database column 10_30 SqlType(DOUBLE), Default(None)
   *  @param `3060` Database column 30_60 SqlType(DOUBLE), Default(None)
   *  @param `60` Database column 60 SqlType(DOUBLE), Default(None) */
  case class SessionAggrStatRow(id: Int, sessionCount: Option[Int] = None, `1s3s`: Option[Double] = None, `4s6s`: Option[Double] = None, `7s9s`: Option[Double] = None, `10s30s`: Option[Double] = None, `30s60s`: Option[Double] = None, `1m3m`: Option[Double] = None, `3m10m`: Option[Double] = None, `10m30m`: Option[Double] = None, `30m`: Option[Double] = None, `13`: Option[Double] = None, `46`: Option[Double] = None, `79`: Option[Double] = None, `1030`: Option[Double] = None, `3060`: Option[Double] = None, `60`: Option[Double] = None)
  /** GetResult implicit for fetching SessionAggrStatRow objects using plain SQL queries */
  implicit def GetResultSessionAggrStatRow(implicit e0: GR[Int], e1: GR[Option[Int]], e2: GR[Option[Double]]): GR[SessionAggrStatRow] = GR{
    prs => import prs._
    SessionAggrStatRow.tupled((<<[Int], <<?[Int], <<?[Double], <<?[Double], <<?[Double], <<?[Double], <<?[Double], <<?[Double], <<?[Double], <<?[Double], <<?[Double], <<?[Double], <<?[Double], <<?[Double], <<?[Double], <<?[Double], <<?[Double]))
  }
  /** Table description of table session_aggr_stat. Objects of this class serve as prototypes for rows in queries. */
  class SessionAggrStat(_tableTag: Tag) extends profile.api.Table[SessionAggrStatRow](_tableTag, Some("spark_session"), "session_aggr_stat") {
    def * = (id, sessionCount, `1s3s`, `4s6s`, `7s9s`, `10s30s`, `30s60s`, `1m3m`, `3m10m`, `10m30m`, `30m`, `13`, `46`, `79`, `1030`, `3060`, `60`) <> (SessionAggrStatRow.tupled, SessionAggrStatRow.unapply)
    /** Maps whole row to an option. Useful for outer joins. */
    def ? = (Rep.Some(id), sessionCount, `1s3s`, `4s6s`, `7s9s`, `10s30s`, `30s60s`, `1m3m`, `3m10m`, `10m30m`, `30m`, `13`, `46`, `79`, `1030`, `3060`, `60`).shaped.<>({r=>import r._; _1.map(_=> SessionAggrStatRow.tupled((_1.get, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, _17)))}, (_:Any) =>  throw new Exception("Inserting into ? projection not supported."))

    /** Database column id SqlType(INT), PrimaryKey */
    val id: Rep[Int] = column[Int]("id", O.PrimaryKey)
    /** Database column session_count SqlType(INT), Default(None) */
    val sessionCount: Rep[Option[Int]] = column[Option[Int]]("session_count", O.Default(None))
    /** Database column 1s_3s SqlType(DOUBLE), Default(None) */
    val `1s3s`: Rep[Option[Double]] = column[Option[Double]]("1s_3s", O.Default(None))
    /** Database column 4s_6s SqlType(DOUBLE), Default(None) */
    val `4s6s`: Rep[Option[Double]] = column[Option[Double]]("4s_6s", O.Default(None))
    /** Database column 7s_9s SqlType(DOUBLE), Default(None) */
    val `7s9s`: Rep[Option[Double]] = column[Option[Double]]("7s_9s", O.Default(None))
    /** Database column 10s_30s SqlType(DOUBLE), Default(None) */
    val `10s30s`: Rep[Option[Double]] = column[Option[Double]]("10s_30s", O.Default(None))
    /** Database column 30s_60s SqlType(DOUBLE), Default(None) */
    val `30s60s`: Rep[Option[Double]] = column[Option[Double]]("30s_60s", O.Default(None))
    /** Database column 1m_3m SqlType(DOUBLE), Default(None) */
    val `1m3m`: Rep[Option[Double]] = column[Option[Double]]("1m_3m", O.Default(None))
    /** Database column 3m_10m SqlType(DOUBLE), Default(None) */
    val `3m10m`: Rep[Option[Double]] = column[Option[Double]]("3m_10m", O.Default(None))
    /** Database column 10m_30m SqlType(DOUBLE), Default(None) */
    val `10m30m`: Rep[Option[Double]] = column[Option[Double]]("10m_30m", O.Default(None))
    /** Database column 30m SqlType(DOUBLE), Default(None) */
    val `30m`: Rep[Option[Double]] = column[Option[Double]]("30m", O.Default(None))
    /** Database column 1_3 SqlType(DOUBLE), Default(None) */
    val `13`: Rep[Option[Double]] = column[Option[Double]]("1_3", O.Default(None))
    /** Database column 4_6 SqlType(DOUBLE), Default(None) */
    val `46`: Rep[Option[Double]] = column[Option[Double]]("4_6", O.Default(None))
    /** Database column 7_9 SqlType(DOUBLE), Default(None) */
    val `79`: Rep[Option[Double]] = column[Option[Double]]("7_9", O.Default(None))
    /** Database column 10_30 SqlType(DOUBLE), Default(None) */
    val `1030`: Rep[Option[Double]] = column[Option[Double]]("10_30", O.Default(None))
    /** Database column 30_60 SqlType(DOUBLE), Default(None) */
    val `3060`: Rep[Option[Double]] = column[Option[Double]]("30_60", O.Default(None))
    /** Database column 60 SqlType(DOUBLE), Default(None) */
    val `60`: Rep[Option[Double]] = column[Option[Double]]("60", O.Default(None))
  }
  /** Collection-like TableQuery object for table SessionAggrStat */
  lazy val SessionAggrStat = new TableQuery(tag => new SessionAggrStat(tag))

  /** Entity class storing rows of table SessionDetail
   *  @param id Database column id SqlType(INT), PrimaryKey
   *  @param userId Database column user_id SqlType(INT), Default(None)
   *  @param sessionId Database column session_id SqlType(VARCHAR), Length(255,true), Default(None)
   *  @param pageId Database column page_id SqlType(INT), Default(None)
   *  @param actionTime Database column action_time SqlType(VARCHAR), Length(255,true), Default(None)
   *  @param searchKeyword Database column search_keyword SqlType(VARCHAR), Length(255,true), Default(None)
   *  @param clickCategoryId Database column click_category_id SqlType(INT), Default(None)
   *  @param clickProductId Database column click_product_id SqlType(INT), Default(None)
   *  @param orderCategoryIds Database column order_category_ids SqlType(VARCHAR), Length(255,true), Default(None)
   *  @param orderProductIds Database column order_product_ids SqlType(VARCHAR), Length(255,true), Default(None)
   *  @param payCategoryIds Database column pay_category_ids SqlType(VARCHAR), Length(255,true), Default(None)
   *  @param payProductIds Database column pay_product_ids SqlType(VARCHAR), Length(255,true), Default(None) */
  case class SessionDetailRow(id: Int, userId: Option[Int] = None, sessionId: Option[String] = None, pageId: Option[Int] = None, actionTime: Option[String] = None, searchKeyword: Option[String] = None, clickCategoryId: Option[Int] = None, clickProductId: Option[Int] = None, orderCategoryIds: Option[String] = None, orderProductIds: Option[String] = None, payCategoryIds: Option[String] = None, payProductIds: Option[String] = None)
  /** GetResult implicit for fetching SessionDetailRow objects using plain SQL queries */
  implicit def GetResultSessionDetailRow(implicit e0: GR[Int], e1: GR[Option[Int]], e2: GR[Option[String]]): GR[SessionDetailRow] = GR{
    prs => import prs._
    SessionDetailRow.tupled((<<[Int], <<?[Int], <<?[String], <<?[Int], <<?[String], <<?[String], <<?[Int], <<?[Int], <<?[String], <<?[String], <<?[String], <<?[String]))
  }
  /** Table description of table session_detail. Objects of this class serve as prototypes for rows in queries. */
  class SessionDetail(_tableTag: Tag) extends profile.api.Table[SessionDetailRow](_tableTag, Some("spark_session"), "session_detail") {
    def * = (id, userId, sessionId, pageId, actionTime, searchKeyword, clickCategoryId, clickProductId, orderCategoryIds, orderProductIds, payCategoryIds, payProductIds) <> (SessionDetailRow.tupled, SessionDetailRow.unapply)
    /** Maps whole row to an option. Useful for outer joins. */
    def ? = (Rep.Some(id), userId, sessionId, pageId, actionTime, searchKeyword, clickCategoryId, clickProductId, orderCategoryIds, orderProductIds, payCategoryIds, payProductIds).shaped.<>({r=>import r._; _1.map(_=> SessionDetailRow.tupled((_1.get, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12)))}, (_:Any) =>  throw new Exception("Inserting into ? projection not supported."))

    /** Database column id SqlType(INT), PrimaryKey */
    val id: Rep[Int] = column[Int]("id", O.PrimaryKey)
    /** Database column user_id SqlType(INT), Default(None) */
    val userId: Rep[Option[Int]] = column[Option[Int]]("user_id", O.Default(None))
    /** Database column session_id SqlType(VARCHAR), Length(255,true), Default(None) */
    val sessionId: Rep[Option[String]] = column[Option[String]]("session_id", O.Length(255,varying=true), O.Default(None))
    /** Database column page_id SqlType(INT), Default(None) */
    val pageId: Rep[Option[Int]] = column[Option[Int]]("page_id", O.Default(None))
    /** Database column action_time SqlType(VARCHAR), Length(255,true), Default(None) */
    val actionTime: Rep[Option[String]] = column[Option[String]]("action_time", O.Length(255,varying=true), O.Default(None))
    /** Database column search_keyword SqlType(VARCHAR), Length(255,true), Default(None) */
    val searchKeyword: Rep[Option[String]] = column[Option[String]]("search_keyword", O.Length(255,varying=true), O.Default(None))
    /** Database column click_category_id SqlType(INT), Default(None) */
    val clickCategoryId: Rep[Option[Int]] = column[Option[Int]]("click_category_id", O.Default(None))
    /** Database column click_product_id SqlType(INT), Default(None) */
    val clickProductId: Rep[Option[Int]] = column[Option[Int]]("click_product_id", O.Default(None))
    /** Database column order_category_ids SqlType(VARCHAR), Length(255,true), Default(None) */
    val orderCategoryIds: Rep[Option[String]] = column[Option[String]]("order_category_ids", O.Length(255,varying=true), O.Default(None))
    /** Database column order_product_ids SqlType(VARCHAR), Length(255,true), Default(None) */
    val orderProductIds: Rep[Option[String]] = column[Option[String]]("order_product_ids", O.Length(255,varying=true), O.Default(None))
    /** Database column pay_category_ids SqlType(VARCHAR), Length(255,true), Default(None) */
    val payCategoryIds: Rep[Option[String]] = column[Option[String]]("pay_category_ids", O.Length(255,varying=true), O.Default(None))
    /** Database column pay_product_ids SqlType(VARCHAR), Length(255,true), Default(None) */
    val payProductIds: Rep[Option[String]] = column[Option[String]]("pay_product_ids", O.Length(255,varying=true), O.Default(None))
  }
  /** Collection-like TableQuery object for table SessionDetail */
  lazy val SessionDetail = new TableQuery(tag => new SessionDetail(tag))

  /** Entity class storing rows of table SessionRandomExtract
   *  @param id Database column id SqlType(INT), PrimaryKey
   *  @param sessionId Database column session_id SqlType(VARCHAR), Length(255,true), Default(None)
   *  @param startTime Database column start_time SqlType(VARCHAR), Length(50,true), Default(None)
   *  @param endTime Database column end_time SqlType(VARCHAR), Length(50,true), Default(None)
   *  @param searchKeywords Database column search_keywords SqlType(VARCHAR), Length(255,true), Default(None) */
  case class SessionRandomExtractRow(id: Int, sessionId: Option[String] = None, startTime: Option[String] = None, endTime: Option[String] = None, searchKeywords: Option[String] = None)
  /** GetResult implicit for fetching SessionRandomExtractRow objects using plain SQL queries */
  implicit def GetResultSessionRandomExtractRow(implicit e0: GR[Int], e1: GR[Option[String]]): GR[SessionRandomExtractRow] = GR{
    prs => import prs._
    SessionRandomExtractRow.tupled((<<[Int], <<?[String], <<?[String], <<?[String], <<?[String]))
  }
  /** Table description of table session_random_extract. Objects of this class serve as prototypes for rows in queries. */
  class SessionRandomExtract(_tableTag: Tag) extends profile.api.Table[SessionRandomExtractRow](_tableTag, Some("spark_session"), "session_random_extract") {
    def * = (id, sessionId, startTime, endTime, searchKeywords) <> (SessionRandomExtractRow.tupled, SessionRandomExtractRow.unapply)
    /** Maps whole row to an option. Useful for outer joins. */
    def ? = (Rep.Some(id), sessionId, startTime, endTime, searchKeywords).shaped.<>({r=>import r._; _1.map(_=> SessionRandomExtractRow.tupled((_1.get, _2, _3, _4, _5)))}, (_:Any) =>  throw new Exception("Inserting into ? projection not supported."))

    /** Database column id SqlType(INT), PrimaryKey */
    val id: Rep[Int] = column[Int]("id", O.PrimaryKey)
    /** Database column session_id SqlType(VARCHAR), Length(255,true), Default(None) */
    val sessionId: Rep[Option[String]] = column[Option[String]]("session_id", O.Length(255,varying=true), O.Default(None))
    /** Database column start_time SqlType(VARCHAR), Length(50,true), Default(None) */
    val startTime: Rep[Option[String]] = column[Option[String]]("start_time", O.Length(50,varying=true), O.Default(None))
    /** Database column end_time SqlType(VARCHAR), Length(50,true), Default(None) */
    val endTime: Rep[Option[String]] = column[Option[String]]("end_time", O.Length(50,varying=true), O.Default(None))
    /** Database column search_keywords SqlType(VARCHAR), Length(255,true), Default(None) */
    val searchKeywords: Rep[Option[String]] = column[Option[String]]("search_keywords", O.Length(255,varying=true), O.Default(None))
  }
  /** Collection-like TableQuery object for table SessionRandomExtract */
  lazy val SessionRandomExtract = new TableQuery(tag => new SessionRandomExtract(tag))

  /** Entity class storing rows of table Top10Category
   *  @param id Database column id SqlType(INT), PrimaryKey
   *  @param categoryId Database column category_id SqlType(INT), Default(None)
   *  @param clickCount Database column click_count SqlType(INT), Default(None)
   *  @param orderCount Database column order_count SqlType(INT), Default(None)
   *  @param payCount Database column pay_count SqlType(INT), Default(None) */
  case class Top10CategoryRow(id: Int, categoryId: Option[Int] = None, clickCount: Option[Int] = None, orderCount: Option[Int] = None, payCount: Option[Int] = None)
  /** GetResult implicit for fetching Top10CategoryRow objects using plain SQL queries */
  implicit def GetResultTop10CategoryRow(implicit e0: GR[Int], e1: GR[Option[Int]]): GR[Top10CategoryRow] = GR{
    prs => import prs._
    Top10CategoryRow.tupled((<<[Int], <<?[Int], <<?[Int], <<?[Int], <<?[Int]))
  }
  /** Table description of table top10_category. Objects of this class serve as prototypes for rows in queries. */
  class Top10Category(_tableTag: Tag) extends profile.api.Table[Top10CategoryRow](_tableTag, Some("spark_session"), "top10_category") {
    def * = (id, categoryId, clickCount, orderCount, payCount) <> (Top10CategoryRow.tupled, Top10CategoryRow.unapply)
    /** Maps whole row to an option. Useful for outer joins. */
    def ? = (Rep.Some(id), categoryId, clickCount, orderCount, payCount).shaped.<>({r=>import r._; _1.map(_=> Top10CategoryRow.tupled((_1.get, _2, _3, _4, _5)))}, (_:Any) =>  throw new Exception("Inserting into ? projection not supported."))

    /** Database column id SqlType(INT), PrimaryKey */
    val id: Rep[Int] = column[Int]("id", O.PrimaryKey)
    /** Database column category_id SqlType(INT), Default(None) */
    val categoryId: Rep[Option[Int]] = column[Option[Int]]("category_id", O.Default(None))
    /** Database column click_count SqlType(INT), Default(None) */
    val clickCount: Rep[Option[Int]] = column[Option[Int]]("click_count", O.Default(None))
    /** Database column order_count SqlType(INT), Default(None) */
    val orderCount: Rep[Option[Int]] = column[Option[Int]]("order_count", O.Default(None))
    /** Database column pay_count SqlType(INT), Default(None) */
    val payCount: Rep[Option[Int]] = column[Option[Int]]("pay_count", O.Default(None))
  }
  /** Collection-like TableQuery object for table Top10Category */
  lazy val Top10Category = new TableQuery(tag => new Top10Category(tag))

  /** Entity class storing rows of table Top10CategorySession
   *  @param id Database column id SqlType(INT UNSIGNED), AutoInc, PrimaryKey
   *  @param categoryId Database column category_id SqlType(INT)
   *  @param sessionId Database column session_id SqlType(VARCHAR), Length(255,true)
   *  @param clickCount Database column click_count SqlType(INT) */
  case class Top10CategorySessionRow(id: Int, categoryId: Int, sessionId: String, clickCount: Int)
  /** GetResult implicit for fetching Top10CategorySessionRow objects using plain SQL queries */
  implicit def GetResultTop10CategorySessionRow(implicit e0: GR[Int], e1: GR[String]): GR[Top10CategorySessionRow] = GR{
    prs => import prs._
    Top10CategorySessionRow.tupled((<<[Int], <<[Int], <<[String], <<[Int]))
  }
  /** Table description of table top10_category_session. Objects of this class serve as prototypes for rows in queries. */
  class Top10CategorySession(_tableTag: Tag) extends profile.api.Table[Top10CategorySessionRow](_tableTag, Some("spark_session"), "top10_category_session") {
    def * = (id, categoryId, sessionId, clickCount) <> (Top10CategorySessionRow.tupled, Top10CategorySessionRow.unapply)
    /** Maps whole row to an option. Useful for outer joins. */
    def ? = (Rep.Some(id), Rep.Some(categoryId), Rep.Some(sessionId), Rep.Some(clickCount)).shaped.<>({r=>import r._; _1.map(_=> Top10CategorySessionRow.tupled((_1.get, _2.get, _3.get, _4.get)))}, (_:Any) =>  throw new Exception("Inserting into ? projection not supported."))

    /** Database column id SqlType(INT UNSIGNED), AutoInc, PrimaryKey */
    val id: Rep[Int] = column[Int]("id", O.AutoInc, O.PrimaryKey)
    /** Database column category_id SqlType(INT) */
    val categoryId: Rep[Int] = column[Int]("category_id")
    /** Database column session_id SqlType(VARCHAR), Length(255,true) */
    val sessionId: Rep[String] = column[String]("session_id", O.Length(255,varying=true))
    /** Database column click_count SqlType(INT) */
    val clickCount: Rep[Int] = column[Int]("click_count")
  }
  /** Collection-like TableQuery object for table Top10CategorySession */
  lazy val Top10CategorySession = new TableQuery(tag => new Top10CategorySession(tag))
}
