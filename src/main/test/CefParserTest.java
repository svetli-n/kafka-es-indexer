import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CefParserTest {

  @Test
  public void testParser() {
    Assertions.assertEquals("{}", CefParser.toJson("34"));
    Assertions.assertEquals("{}", CefParser.toJson(""));
  }

}
