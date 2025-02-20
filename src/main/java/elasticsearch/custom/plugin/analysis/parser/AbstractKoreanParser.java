package elasticsearch.custom.plugin.analysis.parser;

import elasticsearch.custom.plugin.analysis.dto.Korean;
import elasticsearch.custom.plugin.common.util.JamoUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractKoreanParser<T> {

    /**
     * 파싱된 한글을 반환.(extends 를 통해 용도에 맞게 재정의)
     * @param token
     * @return
     */
    public abstract T parse(String token);

    /**
     * 입력된 토큰이 한국어 일 경우 초성, 중성, 종성으로 분리하여 반환,
     * 이외의 경우 그대로 반환.
     * @param token
     * @return List<Korean>
     */
    protected List<Korean> parser(String token) {
        if (StringUtils.isBlank(token)) {
            return new ArrayList<>();
        }

        List<Korean> result = new ArrayList<>();

        // 토큰을 한 글자씩 처리
        char[] arrChar = token.toCharArray();
        for(char ch: arrChar) {
            // 처리 할 글자의 유니코드 인덱스( 가(0xAC00) 첫 번째 한글부터 떨어진 순번 )
            // ex. 가 = 0, 각 = 1, 갂 = 2, 갃 = 3 ...
            //     개 = 28, 객 = 29, 갞 = 30, 갟 = 31 ...
            //     까 = 588, 깍 = 589, 깎 = 590, 깏 = 591 ...
            char unicodeIndex = (char)(ch - JamoUtil.START_KOREA_UNICODE);

            // 한글 유니코드 범위 : 0xAC00 ~ 0xD7AF (11184개)
            // 한글 유니코드 여부 확인
            if(unicodeIndex >= 0 && unicodeIndex <= 11184) {

                // 초성
                int idxChoSeong = unicodeIndex / (JamoUtil.UNICODE_JUNG_SEONG_CNT * JamoUtil.UNICODE_JONG_SEONG_CNT);
                char choSeong = JamoUtil.UNICODE_CHO_SEONG[idxChoSeong];

                // 중성
                int idxJungSeong = unicodeIndex % (JamoUtil.UNICODE_JUNG_SEONG_CNT * JamoUtil.UNICODE_JONG_SEONG_CNT) / JamoUtil.UNICODE_JONG_SEONG_CNT;
                char jungSeong = JamoUtil.UNICODE_JUNG_SEONG[idxJungSeong];

                // 종성
                int idxJongSeong = unicodeIndex % (JamoUtil.UNICODE_JUNG_SEONG_CNT * JamoUtil.UNICODE_JONG_SEONG_CNT) % JamoUtil.UNICODE_JONG_SEONG_CNT;
                char jongSeong = JamoUtil.UNICODE_JONG_SEONG[idxJongSeong];

                // 분리된 음절 처리
                result.add(processForKorean(choSeong, jungSeong, jongSeong, idxChoSeong, idxJungSeong, idxJongSeong));
            } else {
                // 한글이 아닌 한 글자 처리
                result.add(processForOther(ch));
            }
        }

        // 토큰 분석 결과 반환
        return result;
    }

    /**
     * 한글 문자 처리 함수
     * @param choSeong
     * @param jungSeong
     * @param jongSeong
     */
    private Korean processForKorean(char choSeong, char jungSeong, char jongSeong, int idxChoSeong, int idxJungSeong, int idxJongSeong) {
        Korean korean = new Korean();
        korean.setCombinedKorean();
        korean.setChoSeong(choSeong);
        korean.setJungSeong(jungSeong);
        korean.setJongSeong(jongSeong);
        korean.setIdxChoSeong(idxChoSeong);
        korean.setIdxJungSeong(idxJungSeong);
        korean.setIdxJongSeong(idxJongSeong);
        return korean;
    }

    /**
     * 한글 제외 문자 처리 함수
     * @param ch
     */
    private Korean processForOther(char ch) {
        Korean korean = new Korean();
        korean.setOtherChar(ch);
        return korean;
    }

}
