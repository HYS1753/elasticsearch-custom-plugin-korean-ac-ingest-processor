package elasticsearch.custom.plugin.analysis.converter;

import elasticsearch.custom.plugin.common.util.JamoUtil;
import elasticsearch.custom.plugin.common.util.KeyboardUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

public class Eng2KorConverter {

    private final boolean convertSingleKoreanLetter;

    public Eng2KorConverter() {
        this.convertSingleKoreanLetter = true;
    }

    public Eng2KorConverter(boolean convertSingleKoreanLetter) {
        this.convertSingleKoreanLetter = convertSingleKoreanLetter;
    }

    /**
     * 영문 -> 한국어 변환한다.
     * @param token
     * @return
     */
    public String convert(String token) {
        StringBuilder sb = new StringBuilder();

        // 문자열 한글자씩 잘라서 처리.
        String word = token.trim();
        for (int index = 0; index < word.length(); index++) {
            // index 초과 방지
            if (index >= word.length()) {
                break;
            }
            char charWord = word.charAt(index);
            boolean isEng = String.valueOf(charWord).matches(KeyboardUtil.ENG_REGEX);

            if (isEng) {
                try {
                    // 조합된 한국어의 경우
                    Map<String, Integer> combinedKorean = getEng2CombinedKorChar(index, word);
                    Integer resultWord = combinedKorean.get("word");
                    Integer nextIndex = combinedKorean.get("nextIndex");
                    if (index != nextIndex) { // 조합된 한국어가 있음
                        sb.append((char) resultWord.intValue());
                        index = nextIndex;
                    } else {                  // 조합된 한국어가 없음
                        if (this.convertSingleKoreanLetter) {
                            char separatedEngChar = (char) resultWord.intValue();
                            String separatedEngStr = String.valueOf(separatedEngChar);
                            sb.append(getEng2SeparatedKorChar(separatedEngStr));
                        } else {
                            sb.append((char) resultWord.intValue());
                        }
                    }
                } catch (Exception e) {}
            } else {
                sb.append(charWord);
            }

        }
        return sb.toString();
    }

    /**
     * index 부터 시작하여 조합되는 한국어가 있는지 확인
     * 조합되는 결과가 있을 경우 word = 조합된 한국어, nextIndex = 조합된 영문 개수 포함 순번
     *     (rk(가) 가 입력되면 두개 영문이 하나의 한글로 치환 된 것이므로, 1이 들어가 한번 더 확인 한 것으로 간주)
     * 조합되는 결과가 없을 경우 word = 해당 회차의 문자, nextIndex = 해당 회차 순번
     * @param index
     * @param word
     * @return
     */
    private Map<String, Integer> getEng2CombinedKorChar(int index, String word) {
        // int 기본형 변수이므로 값 복사로 전달. 함수 호출 전 변수 변경 없음. 해당 메서드 내에서만 변동.
        Map<String, Integer> result = new HashMap<>();
        Integer initIndex = index;

        int choSeong = 0;
        int jungSeong = 0;
        int jongSeong = 0;

        try {
            // 초성
            Map<String, Integer> mChoSeong = KeyboardUtil.getInfoForChoSeong(index, word);
            choSeong = mChoSeong.get("code");
            index = mChoSeong.get("idx");
            // 중성
            Map<String, Integer> mJungSeong = KeyboardUtil.getInfoForJungSeong(index, word);
            jungSeong = mJungSeong.get("code");
            index = mJungSeong.get("idx");
            // 종성
            Map<String, Integer> mJongSeong = KeyboardUtil.getInfoForJongSeong(index, word);
            jongSeong = mJongSeong.get("code");
            index = mJongSeong.get("idx");

            // 한글이 조합 되었을 경우
            if (choSeong >= 0 && jungSeong >= 0) {
                result.put("word", JamoUtil.START_KOREA_UNICODE + choSeong + jungSeong + jongSeong);
                result.put("nextIndex", index);
            } else {
                result.put("word", (int) word.charAt(initIndex));
                result.put("nextIndex", initIndex);
            }
        } catch (Exception e) {}

        return result;
    }

    /**
     * 자소 분리 된 영어 한글로 변환.
     * @param key
     * @return
     */
    private String getEng2SeparatedKorChar(String key) {

        return IntStream.range(0, KeyboardUtil.KEYBOARD_KEY_ENG.length)
                .filter(i -> KeyboardUtil.KEYBOARD_KEY_ENG[i].equals(key))
                .mapToObj(i -> KeyboardUtil.KEYBOARD_KEY_KOR[i])
                .findFirst()
                .orElse(key);
    }
}
