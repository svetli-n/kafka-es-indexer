import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;

public class CefParser {

  public static String toJson(String cef) {
    String[] headers = cef.split("\\|");
    String payload = headers[headers.length-1].trim();
    String[] data = payload.split(" ");

    Map<String, String> map = new HashMap<>();
    if (headers.length == 7) {
      map.put("CEFVersion", headers[0]);
      map.put("DeviceVendor", headers[1]);
      map.put("DeviceProduct", headers[2]);
      map.put("DeviceVersion", headers[3]);
      map.put("SignatureId", headers[4]);
      map.put("Name", headers[5]);
      map.put("Severity", headers[6]);
    }

    for (String part: data) {
      String[] temp = part.split("=");
      if (temp.length == 2)
        map.put(temp[0].replace(".", "_dot_"), temp[1]);
    }

    try {
      return new ObjectMapper().writeValueAsString(map);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      return "{}";
    }

  }


}
