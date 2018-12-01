package com.monkeyquant.qsh.model;

import com.monkeyquant.jte.primitives.interfaces.IBookState;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
public class BookStateEvent {
  java.sql.Timestamp time;
  IBookState bookState;
}
