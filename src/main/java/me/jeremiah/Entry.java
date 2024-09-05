package me.jeremiah;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import me.jeremiah.utils.EntryGeneratorUtils;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;

import java.sql.PreparedStatement;
import java.sql.SQLException;

@AllArgsConstructor
@Setter
@Getter
public class Entry implements DatabaseSerializable {

  private final int id;
  private String firstName;
  private char middleInitial;
  private String lastName;

  public Entry(int id) {
    this.id = id;
    this.firstName = EntryGeneratorUtils.generateFirstName();
    this.middleInitial = EntryGeneratorUtils.generateMiddleInitial();
    this.lastName = EntryGeneratorUtils.generateLastName();
  }

  @Override
  public void serializeInsert(@NotNull PreparedStatement preparedStatement) throws SQLException {
    preparedStatement.setInt(1, id);
    preparedStatement.setString(2, firstName);
    preparedStatement.setString(3, String.valueOf(middleInitial));
    preparedStatement.setString(4, lastName);
  }

  @Override
public void serializeUpdate(@NotNull PreparedStatement preparedStatement) throws SQLException {
    preparedStatement.setString(1, firstName);
    preparedStatement.setString(2, String.valueOf(middleInitial));
    preparedStatement.setString(3, lastName);
    preparedStatement.setInt(4, id);
  }

  @Override
  public @NotNull Document toDocument() {
    return new Document()
      .append("id", id)
      .append("first_name", firstName)
      .append("middle_initial", String.valueOf(middleInitial))
      .append("last_name", lastName);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    Entry entry = (Entry) obj;
    return id == entry.id &&
      middleInitial == entry.middleInitial &&
      firstName.equals(entry.firstName) &&
      lastName.equals(entry.lastName);
  }

}
