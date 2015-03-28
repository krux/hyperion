package com.krux.hyperion.objects

/**
 * This does not relate to any AWS datapipeline database object. The purpose of this object is to
 * bring some consistency that like RedshiftDataNode that reference to RedshiftDatabase
 * for connection details. SqlDataNode should also reference database liked object for
 * connection details rather than specify username, password etc. in the DataNode object.
 */
trait MySqlDatabase {
  def username: String
  def `*password`: String
  def connectionString: String
}
