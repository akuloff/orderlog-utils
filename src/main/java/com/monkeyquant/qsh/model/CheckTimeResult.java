package com.monkeyquant.qsh.model;

import lombok.Builder;
import lombok.Getter;

import java.util.Date;

@Getter
@Builder
public class CheckTimeResult {
  private boolean doWrite;
  private Date quantDate;
}
