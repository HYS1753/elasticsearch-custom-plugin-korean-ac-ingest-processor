package elasticsearch.custom.plugin.analysis.converter;

import elasticsearch.custom.plugin.analysis.dto.Korean;
import elasticsearch.custom.plugin.analysis.parser.KoreanParser;
import elasticsearch.custom.plugin.common.enumeration.CodeType;
import elasticsearch.custom.plugin.common.util.JamoUtil;
import elasticsearch.custom.plugin.common.util.KeyboardUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.IntStream;

public class Kor2EngConverter {

    private final boolean convertSingleKoreanLetter;

    public Kor2EngConverter() {
        this.convertSingleKoreanLetter = true;
    }

    public Kor2EngConverter(boolean convertSingleKoreanLetter) {
        this.convertSingleKoreanLetter = convertSingleKoreanLetter;
    }

    public String convert(String token) {
        StringBuilder sb = new StringBuilder();

        KoreanParser parser = new KoreanParser();
        List<Korean> koreans = parser.parse(token);

        for (Korean korean : koreans) {
            try {
                // 조합된(Combined) 한국어 파싱
                if (korean.isCombinedKorean()) {
                    // 초성
                    String strChoSeong = getCombinedKor2EngChar(CodeType.CHOSEONG, korean.getIdxChoSeong());
                    // 중성
                    String strJungSeong = getCombinedKor2EngChar(CodeType.JUNGSEONG, korean.getIdxJungSeong());
                    // 종성
                    String strJongSeong = getCombinedKor2EngChar(CodeType.JONGSEONG, korean.getIdxJongSeong());

                    sb.append(strChoSeong).append(strJungSeong);
                    if (StringUtils.isNotEmpty(strJongSeong)) {
                        sb.append(strJongSeong);
                    }
                } else {
                    char otherChar = korean.getOtherChar();
                    int intOtherChar = (int) otherChar;
                    boolean isJamoKor = (intOtherChar >= JamoUtil.START_JAMO_KOREA_UNICODE
                                      && intOtherChar <= JamoUtil.END_JAMO_KOREA_UNICODE);

                    // 단일 자음, 모음 한국어 파싱
                    if (isJamoKor && convertSingleKoreanLetter) {
                        String strJamo = getSeparatedKor2EngChar(String.valueOf(otherChar));
                        sb.append(strJamo);
                    } else {
                        // 한글이 아닐 경우 그대로 추가
                        sb.append(otherChar);
                    }
                }
            } catch (Exception e) {}
        }

        return sb.toString();
    }

    /**
     * 조합 된 한글의 초성, 중성, 종성 별 영어 변환.
     * @param type
     * @param pos
     * @return
     */
    private String getCombinedKor2EngChar(CodeType type, int pos) {

        return switch (type) {
            case CHOSEONG -> KeyboardUtil.KEYBOARD_CHO_SEONG[pos];
            case JUNGSEONG -> KeyboardUtil.KEYBOARD_JUNG_SEONG[pos];
            case JONGSEONG -> {
                if (pos > 0) {
                    yield KeyboardUtil.KEYBOARD_JONG_SEONG[pos - 1];
                }
                yield "";
            }
        };
    }

    /**
     * 자소 분리 된 한글의 영어 변환.
     * @param key
     * @return
     */
    private String getSeparatedKor2EngChar(String key) {

        return IntStream.range(0, KeyboardUtil.KEYBOARD_KEY_KOR.length)
                .filter(i -> KeyboardUtil.KEYBOARD_KEY_KOR[i].equals(key))
                .mapToObj(i -> KeyboardUtil.KEYBOARD_KEY_ENG[i])
                .findFirst()
                .orElse("");
    }
}
