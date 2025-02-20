package elasticsearch.custom.plugin.processor;

import elasticsearch.custom.plugin.analysis.converter.Eng2KorConverter;
import elasticsearch.custom.plugin.analysis.converter.Kor2EngConverter;
import elasticsearch.custom.plugin.analysis.parser.KoreanChoSeongParser;
import elasticsearch.custom.plugin.analysis.parser.KoreanJamoParser;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.*;

/**
 * Example of adding an ingest processor with a plugin.
 */
public class KoreanAcIngestProcessor extends AbstractProcessor {
    public static final String TYPE = "korean_auto_complete_completion_splitter";
    public static final String TARGET_FIELD_NAME = "target_field";
    public static final String COMPLETION_FIELD_NAME = "completion_field";
    public static final String ACTIVE_CHOSEONG_FILTER = "choseong";
    public static final String ACTIVE_JAMO_FILTER = "jamo";
    public static final String ACTIVE_KOR2ENG_FILTER = "kor2eng";
    public static final String ACTIVE_ENG2KOR_FILTER = "eng2kor";
    public static final String REMOVE_SINGLE_JAEUM_OPTION = "remove_single_jaeum";
    public static final String REMOVE_SINGLE_MOEUM_OPTION = "remove_single_moeum";
    public static final String CONVERT_SINGLE_KOREAN_LETTER_OPTION = "convert_single_korean_letter";

    private final String targetFieldName;
    private final String completionFieldName;
    private final Boolean activeChoseongFilter;
    private final Boolean activeJamoFilter;
    private final Boolean activeKor2engFilter;
    private final Boolean activeEng2korFilter;
    private final Boolean removeSingleJaeum;
    private final Boolean removeSingleMoeum;
    private final Boolean convertSingleKoreanLetter;


    KoreanAcIngestProcessor(String tag, String description,
                            String targetFieldName,
                            String completionFieldName,
                            String activeChoseongFilter,
                            String activeJamoFilter,
                            String activeKor2engFilter,
                            String activeEng2korFilter,
                            String removeSingleJaeum,
                            String removeSingleMoeum,
                            String convertSingleKoreanLetter) {
        super(tag, description);
        this.targetFieldName = targetFieldName;
        this.completionFieldName = completionFieldName;
        // 조건 정의
        this.activeChoseongFilter = Boolean.parseBoolean(activeChoseongFilter);
        this.activeJamoFilter = Boolean.parseBoolean(activeJamoFilter);
        this.activeKor2engFilter = Boolean.parseBoolean(activeKor2engFilter);
        this.activeEng2korFilter = Boolean.parseBoolean(activeEng2korFilter);
        this.removeSingleJaeum = Boolean.parseBoolean(removeSingleJaeum);
        this.removeSingleMoeum = Boolean.parseBoolean(removeSingleMoeum);
        this.convertSingleKoreanLetter = Boolean.parseBoolean(convertSingleKoreanLetter);
    }

    /**
     * 주어진 문자열에서 앞에서부터 한 글자씩 제거하며 리스트를 생성하는 함수
     * 자동완성 내에서 중간부터 검색해도 결과가 나오도록 각 글자별 분리.
     */
    private List<String> generateSubstrings(String text) {
        List<String> substrings = new ArrayList<>();
        for (int i = 0; i < text.length(); i++) {
            substrings.add(text.substring(i));
        }
        return substrings;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        Set<String> completionResult = new HashSet<>();
        Object targetObject = document.getFieldValue(targetFieldName, Object.class, true);

        if (targetObject instanceof String targetText) {
            // 자동완성 프로세스
            // 1. 초성 분리
            if (activeChoseongFilter) {
                KoreanChoSeongParser choseongParser = new KoreanChoSeongParser(removeSingleJaeum, removeSingleMoeum);
                String choseong = choseongParser.parse(targetText);
                completionResult.addAll(generateSubstrings(choseong));
            }
            // 2. 자소 분리
            if (activeJamoFilter) {
                KoreanJamoParser jamoParser = new KoreanJamoParser();
                String jamo = jamoParser.parse(targetText);
                completionResult.addAll(generateSubstrings(jamo));
            }
            // 한타 -> 영타 변환
            if (activeKor2engFilter) {
                Kor2EngConverter kor2EngConverter = new Kor2EngConverter(convertSingleKoreanLetter);
                String kor2Eng = kor2EngConverter.convert(targetText);
                completionResult.addAll(generateSubstrings(kor2Eng));
            }
            // 영타 -> 한타 변환
            if (activeEng2korFilter) {
                Eng2KorConverter eng2KorConverter = new Eng2KorConverter(convertSingleKoreanLetter);
                String eng2Kor = eng2KorConverter.convert(targetText);
                completionResult.addAll(generateSubstrings(eng2Kor));
            }
            // 마지막 결과 지정한 필드 명에 저장.
            document.setFieldValue(completionFieldName, new ArrayList<>(completionResult));
        }
        return document;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static class Factory implements Processor.Factory {

        @Override
        public KoreanAcIngestProcessor create(
                Map<String, Processor.Factory> registry,
                String tag,
                String description,
                Map<String, Object> config
        ) {
            String targetFieldName           = ConfigurationUtils.readStringProperty(TYPE, tag, config, TARGET_FIELD_NAME);
            String completionFieldName       = ConfigurationUtils.readStringProperty(TYPE, tag, config, COMPLETION_FIELD_NAME);
            String activeChoseongFilter      = ConfigurationUtils.readStringProperty(TYPE, tag, config, ACTIVE_CHOSEONG_FILTER);
            String activeJamoFilter          = ConfigurationUtils.readStringProperty(TYPE, tag, config, ACTIVE_JAMO_FILTER);
            String activeKor2engFilter       = ConfigurationUtils.readStringProperty(TYPE, tag, config, ACTIVE_KOR2ENG_FILTER);
            String activeEng2korFilter       = ConfigurationUtils.readStringProperty(TYPE, tag, config, ACTIVE_ENG2KOR_FILTER);
            String removeSingleJaeum         = ConfigurationUtils.readStringProperty(TYPE, tag, config, REMOVE_SINGLE_JAEUM_OPTION);
            String removeSingleMoeum         = ConfigurationUtils.readStringProperty(TYPE, tag, config, REMOVE_SINGLE_MOEUM_OPTION);
            String convertSingleKoreanLetter = ConfigurationUtils.readStringProperty(TYPE, tag, config, CONVERT_SINGLE_KOREAN_LETTER_OPTION);

            return new KoreanAcIngestProcessor(tag, description,
                                                targetFieldName,
                                                completionFieldName,
                                                activeChoseongFilter,
                                                activeJamoFilter,
                                                activeKor2engFilter,
                                                activeEng2korFilter,
                                                removeSingleJaeum,
                                                removeSingleMoeum,
                                                convertSingleKoreanLetter);
        }
    }
}