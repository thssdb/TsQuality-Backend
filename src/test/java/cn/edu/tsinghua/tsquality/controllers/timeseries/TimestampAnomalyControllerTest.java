package cn.edu.tsinghua.tsquality.controllers.timeseries;

import static org.hamcrest.Matchers.*;

import cn.edu.tsinghua.tsquality.generators.IoTDBDataGenerator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

@SpringBootTest
@AutoConfigureMockMvc
public class TimestampAnomalyControllerTest {
  private static final int TEST_DATA_SIZE = 100;

  @Autowired private IoTDBDataGenerator dataGenerator;
  @Autowired private MockMvc mockMvc;

  private String path;

  private String getEndpoint() {
    return String.format("/api/v1/timestamp-anomaly?path=%s", path);
  }

  @BeforeEach
  void insertDataWithTimestampAnomalies() throws Exception {
    dataGenerator.generateTimestampAnomalyData(TEST_DATA_SIZE);
    path = IoTDBDataGenerator.paths.getFirst().getFullPath();
  }

  @AfterEach
  void clearData() throws Exception {
    dataGenerator.deleteDatabase();
  }

  @Test
  void test() throws Exception {
    MockHttpServletRequestBuilder request = givenAnomalyDetectionRequest();
    ResultActions result = whenPerformRequest(request);
    thenResultShouldBeOk(result);
    thenResultShouldContainCorrectData(result);
  }

  private MockHttpServletRequestBuilder givenAnomalyDetectionRequest() {
    String endpoint = getEndpoint();
    return MockMvcRequestBuilders.get(endpoint);
  }

  private ResultActions whenPerformRequest(MockHttpServletRequestBuilder request) throws Exception {
    return mockMvc.perform(request);
  }

  private void thenResultShouldBeOk(ResultActions result) throws Exception {
    result.andExpect(MockMvcResultMatchers.status().isOk());
    result.andExpect(MockMvcResultMatchers.jsonPath("$.code", is(0)));
  }

  private void thenResultShouldContainCorrectData(ResultActions result) throws Exception {
    result.andExpect(
        MockMvcResultMatchers.jsonPath("$.data.originalData", hasSize(TEST_DATA_SIZE)));
    result.andExpect(
        MockMvcResultMatchers.jsonPath("$.data.repairedData", hasSize(greaterThan(0))));
  }
}
