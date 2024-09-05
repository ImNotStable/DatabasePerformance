package me.jeremiah;

import org.bson.Document;
import org.jetbrains.annotations.NotNull;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface DatabaseSerializable {

  void serializeInsert(@NotNull PreparedStatement preparedStatement) throws SQLException;

  void serializeUpdate(@NotNull PreparedStatement preparedStatement) throws SQLException;

  @NotNull Document toDocument();

}
