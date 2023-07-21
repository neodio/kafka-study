# kafka-study
- kafka 학습을 위한 프로젝트

## local 실행
1. docker 디렉토리 이동
    - single broker
2. docker-compose 실행 
    - ``` $ docker-compose up ```
3. topic 생성
    - [AdminClientTest#testCreateTopic](./src/test/java/com/kafkastudy/AdminClientTest.java)
4. Test Send API 호출
    - ```
      curl -X POST "http://localhost:8080/send" -H "accept: */*" -H "Content-Type: application/json" -d "{ \"prodNo\": 1, \"prodNm\": \"test Prod\"}"
      ```
    - test-topic에 데이터 등록
5. test-topic을 바라보고 있는 consumer를 통해 데이터 확인
    - [SampleConsumer#listen](./src/main/java/com/kafkastudy/consumer/SampleConsumer.java)

