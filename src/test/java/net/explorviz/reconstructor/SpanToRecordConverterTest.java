package net.explorviz.reconstructor;

import java.time.Instant;
import net.explorviz.landscape.flat.Application;
import net.explorviz.landscape.flat.LandscapeRecord;
import net.explorviz.landscape.flat.Node;
import net.explorviz.trace.EVSpan;
import net.explorviz.trace.Timestamp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SpanToRecordConverterTest {

  private SpanToRecordConverter converter;

  private EVSpan span;
  private LandscapeRecord record;

  @BeforeEach
  void setUp() {
    converter = new SpanToRecordConverter();
    Instant now = Instant.now();
    long duration = 1000L;
    long end = now.toEpochMilli() + duration;
    String token = "tok";
    String hostname = "Host";
    String hostIp = "1.2.3.4";
    String appName = "Test App";
    String appPid = "1234";
    String appLang = "java";


    span = EVSpan.newBuilder()
        .setTraceId("trace")
        .setRequestCount(12)
        .setSpanId("id")
        .setLandscapeToken(token)
        .setStartTime(new Timestamp(now.getEpochSecond(), now.getNano()))
        .setEndTime(end)
        .setDuration(duration)
        .setHostname(hostname)
        .setHostIpAddress(hostIp)
        .setAppName(appName)
        .setAppPid(appPid)
        .setAppLanguage(appLang)
        .setOperationName("foo.bar.TestClass.testMethod()")
        .build();

    record = LandscapeRecord.newBuilder()
        .setLandscapeToken(token)
        .setTimestamp(now.toEpochMilli())
        .setNode(new Node(hostIp, hostname))
        .setApplication(new Application(appName, appPid, appLang))
        .setPackage$("foo.bar")
        .setClass$("TestClass")
        .setMethod("testMethod()")
        .build();

  }

  @Test
  public void convert() {
    LandscapeRecord got = converter.toRecord(span);
    Assertions.assertEquals(got, record, "Converted records does not match expected");
  }

}
