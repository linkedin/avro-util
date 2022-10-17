package com.linkedin.avroutil1.builder;

import build.generated.SpamUser;
import com.linkedin.avroutil1.compatibility.AvroCodecUtil;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroRecordUtil;
import com.linkedin.avroutil1.compatibility.RecordConversionConfig;
import com.linkedin.avroutil1.compatibility.StringRepresentation;
import java.io.File;
import java.net.URL;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class Tester {
  @Test
  public void testSimpleProjectUsingVanilla() throws Exception {
//    File simpleProjectRoot = new File(locateTestProjectsRoot(), "tester");
//    File inputFolder = new File(simpleProjectRoot, "input");
//    File outputFolder = new File(simpleProjectRoot, "output");
//    if (outputFolder.exists()) { //clear output
//      FileUtils.deleteDirectory(outputFolder);
//    }
//    //run the builder
//    SchemaBuilder.main(new String[] {
//        "--input", inputFolder.getAbsolutePath(),
//        "--output", outputFolder.getAbsolutePath()});
//    //see output was generated
//    List<Path> javaFiles = Files.find(outputFolder.toPath(), 5,
//        (path, basicFileAttributes) -> path.getFileName().toString().endsWith(".java")
//    ).collect(Collectors.toList());
//    Assert.assertEquals(javaFiles.size(), 1);
    Schema newSchema = AvroCompatibilityHelper.parse("{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"SpamUser\",\n"
        + "  \"namespace\": \"build.generated\",\n" + "   \"fields\": [\n" + "      {\n" + "        \"name\": \"f\",\n"
        + "        \"type\": {\n" + "                  \"type\": \"enum\",\n"
        + "                  \"name\": \"SpamType\",\n" + "                  \"namespace\": \"build.generated\",\n"
        + "                  \"doc\": \"Enum representing the different types of spam we can classify. BAM owns this definition, it is in common so that it can be included in both messages and tracking events.  Refer to https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/BAM+SpamType for the symbol definitions\",\n"
        + "                  \"symbols\": [\n" + "                    \"ADULT\",\n" + "                    \"JOB\",\n"
        + "                    \"EVENT\",\n" + "                    \"UCV_SPAMMER\",\n"
        + "                    \"PROMOTION\",\n" + "                    \"OTHER_SPAM\",\n"
        + "                    \"BADURL_MALWARE\",\n" + "                    \"BADURL_PHISHING\",\n"
        + "                    \"BADURL_SPAM\",\n" + "                    \"NEW_MEMBER_LIX_BLOCKED\",\n"
        + "                    \"BULK_SPAM\",\n" + "                    \"PROFANITY\",\n"
        + "                    \"PORN\",\n" + "                    \"DUPLICATES\",\n"
        + "                    \"COMMENT_META\",\n" + "                    \"UBIQUITY_SPAM\",\n"
        + "                    \"UBIQUITY_LOWQUALITY\",\n" + "                    \"USER_FLAGGED_SPAM\",\n"
        + "                    \"TOO_MANY_CAPS\",\n" + "                    \"LANGUAGE_MISMATCH\",\n"
        + "                    \"BAD_MEDIA\",\n" + "                    \"MONEY_FRAUD\",\n"
        + "                    \"TOS_VIOLATION\",\n" + "                    \"COMMERCIAL\",\n"
        + "                    \"SPAM_HASHES\",\n" + "                    \"THREATS_OF_VIOLENCE\",\n"
        + "                    \"IRRELEVANT\",\n" + "                    \"GRATUITOUSLY_SHOCKING\",\n"
        + "                    \"HARASSMENT\",\n" + "                    \"IMPERSONATION\",\n"
        + "                    \"PUZZLE\",\n" + "                    \"MEME\",\n"
        + "                    \"MEME_QUOTE_WITH_AUTHOR\",\n" + "                    \"MEME_IRRELEVANT\",\n"
        + "                    \"ADULT_NUDE_ART\",\n" + "                    \"ADULT_BIKINI_LINGERIE\",\n"
        + "                    \"ADULT_MEDICAL\",\n" + "                    \"ADULT_IRRELEVANT\",\n"
        + "                    \"SHOCKING\",\n" + "                    \"SHOCKING_GLOBAL_EVENT\",\n"
        + "                    \"SHOCKING_PERSONAL\",\n" + "                    \"OFFENSIVE\",\n"
        + "                    \"OFFENSIVE_PROFANITY\",\n" + "                    \"PERSONAL\",\n"
        + "                    \"PERSONAL_RELIGIOUS_BELIEF\",\n"
        + "                    \"PERSONAL_POLITICAL_BELIEF\",\n" + "                    \"PERSONAL_PHOTO\",\n"
        + "                    \"PERSONAL_PLEA\",\n" + "                    \"PERSONAL_SELF_PROMOTION\",\n"
        + "                    \"PERSONAL_NON_PAID_FOR_JOB_POSTING\",\n"
        + "                    \"OTHER_LOW_QUALITY\",\n" + "                    \"MALWARE\",\n"
        + "                    \"PHISHING\",\n" + "                    \"GORE\",\n"
        + "                    \"HATE_SPEECH\",\n" + "                    \"PERSONAL_VIDEO\",\n"
        + "                    \"AUDIO_LOW_QUALITY\",\n" + "                    \"MUSIC_VIDEO\",\n"
        + "                    \"MOVIE_VIDEO\",\n" + "                    \"AUDIO_SPAM\",\n"
        + "                    \"POTENTIAL_CHILD_PORN\",\n" + "                    \"COPYRIGHT_VIOLATION\",\n"
        + "                    \"COPYRIGHT_VIOLATION_AUDIO\",\n"
        + "                    \"COPYRIGHT_VIOLATION_VIDEO\",\n" + "                    \"FAKE_NEWS\",\n"
        + "                    \"LOW_QUALITY_NEWS\",\n" + "                    \"UNWANTED_ADVANCES\",\n"
        + "                    \"TERRORISM\",\n" + "                    \"EXTREME_VIOLENCE\",\n"
        + "                    \"DEROGATORY\",\n" + "                    \"VIOLATION_OF_HUMAN_DIGNITY\",\n"
        + "                    \"GENOCIDE_DENIAL\",\n" + "                    \"INFLAMMATORY_CONTENT\",\n"
        + "                    \"INCITEMENT_OF_VIOLENCE\",\n" + "                    \"HARASSMENT_SEXUAL\",\n"
        + "                    \"HARASSMENT_NONSEXUAL\",\n" + "                    \"EGREGIOUS_HARASSMENT_SEXUAL\",\n"
        + "                    \"EGREGIOUS_HARASSMENT_NONSEXUAL\",\n" + "                    \"DISCRIMINATION\",\n"
        + "                    \"MISREPRESENTATION\",\n" + "                    \"GREY_ZONE_CONTENT\",\n"
        + "                    \"RED_ZONE_AUTHOR_CONTENT\",\n" + "                    \"AUSTRIAN_LAW_VIOLATION\",\n"
        + "                    \"ATO_SESSION_GENERATED_CONTENT\",\n"
        + "                    \"MISINFORMATION_DISPUTED_HARM\",\n"
        + "                    \"ILLEGAL_REGULATED_COMMERCIAL\",\n" + "                    \"MISLEADING_CTA\",\n"
        + "                    \"GRAPHIC_CONTENT\",\n" + "                    \"GRAPHIC_NE\",\n"
        + "                    \"GRAPHIC_NE_LQ\",\n" + "                    \"MONEY_SCAM_DECEPTIVE\",\n"
        + "                    \"MONEY_SCAM_WFH\",\n" + "                    \"MONEY_SCAM_UPFRONT_PAYMENT\",\n"
        + "                    \"MONEY_SCAM_IMPERSONATION\",\n" + "                    \"MONEY_SCAM_PHISHING\",\n"
        + "                    \"INFERIOR_INCOMPLETE\",\n" + "                    \"INFERIOR_MULTIPLE_LISTINGS\",\n"
        + "                    \"INFERIOR_BROKEN_LINK\",\n" + "                    \"INFERIOR_COMPANY_MISMATCH\",\n"
        + "                    \"INFERIOR_MISLEADING_POSITION\",\n"
        + "                    \"INFERIOR_JOB_ALREADY_CLOSED\",\n" + "                    \"DRUGS\",\n"
        + "                    \"ALCOHOL\",\n" + "                    \"GAMBLING\",\n"
        + "                    \"FRANCHISES\",\n" + "                    \"MLM\",\n"
        + "                    \"WEAPONS\",\n" + "                    \"CRYPTO_CURRENCY\",\n"
        + "                    \"PROMOTION_SALE_PRODUCT_SERVICE\",\n"
        + "                    \"PROMOTION_SELF_PROMOTION\",\n"
        + "                    \"PROMOTION_TRAINING_COURSES\",\n" + "                    \"DISCRIMINATION_RACE\",\n"
        + "                    \"DISCRIMINATION_AGE\",\n" + "                    \"DISCRIMINATION_SEX\",\n"
        + "                    \"DISCRIMINATION_NATIONALITY\",\n"
        + "                    \"DISCRIMINATION_MARITAL_STATUS\",\n"
        + "                    \"DISCRIMINATION_MILITARY\",\n" + "                    \"PROHIBITED_TRAFFICKING\",\n"
        + "                    \"DISCRIMINATION_OTHER\",\n" + "                    \"INFERIOR_NAME_FIELD_VIOLATION\",\n"
        + "                    \"INFERIOR_OTHER\",\n" + "                    \"INFERIOR_TEST_CONTENT\",\n"
        + "                    \"MONEY_SCAM_ATO\",\n" + "                    \"MONEY_SCAM_OTHER\",\n"
        + "                    \"PROMOTION_OTHER\",\n" + "                    \"NEAR\",\n"
        + "                    \"HUMAN_BODY\",\n" + "                    \"FAKE_ENGAGEMENT\",\n"
        + "                    \"ALCOHOL_TOBACCO\",\n" + "                    \"CIRCUMVENTION\",\n"
        + "                    \"ANIMALS\",\n" + "                    \"PHARMA\",\n" + "                    \"MOB\",\n"
        + "                    \"FABRICATED\",\n" + "                    \"COUNTERFEIT\",\n"
        + "                    \"PRECIOUS\",\n" + "                    \"OCCULT\",\n"
        + "                    \"HUMAN_EXPLOIT\",\n" + "                    \"ILLEGAL_DRUG\",\n"
        + "                    \"MONETIZATION_CSE\",\n" + "                    \"RECREATIONAL_DRUG\",\n"
        + "                    \"LOTTERY\",\n" + "                    \"SPAM_TYPE_MIGRATION\",\n"
        + "                    \"INAUTHENTIC_UNORIGINAL_CONTENT\",\n" + "                    \"UNKNOWN\",\"NEW_ENUM\"\n"
        + "                  ],\n" + "                  \"default\": \"UNKNOWN\"\n" + "                }\n" + "\n"
        + "      }\n" + "   ]\n" + "}\n");
    String newJson = "{\"f\" : \"NEW_ENUM\"}";
    GenericRecord genericRecord = AvroCodecUtil.deserializeAsGeneric(newJson, newSchema, newSchema);
    SpamUser spamUser = new SpamUser();
    SpamUser spamUser2 = AvroRecordUtil.genericRecordToSpecificRecord(genericRecord, null, new RecordConversionConfig(true, true, true, true, StringRepresentation.Utf8, true));
    Assert.assertEquals("UNKNOWN", spamUser2.f.name());

//    SpamType spamType = AvroRecordUtil.genericRecordToSpecificRecord(, null, RecordConversionConfig.ALLOW_ALL);

  }

  private File locateTestProjectsRoot() {
    //the current working directory for test execution varies across gradle and IDEs.
    //as such, we need to get creative to locate the folder
    URL url = Thread.currentThread().getContextClassLoader().getResource("test-projects");
    if (url == null) {
      throw new IllegalStateException("unable to find \"test-projects\" folder");
    }
    if (!url.getProtocol().equals("file")) {
      throw new IllegalStateException(url + " is a " + url.getProtocol() + " and not a file/folder");
    }
    File file = new File(url.getPath()); //getPath() should be an absolute path string
    if (!file.exists()) {
      throw new IllegalStateException("test-projects root folder " + file.getAbsolutePath() + " does not exist");
    }
    if (!file.isDirectory() || !file.canRead()) {
      throw new IllegalStateException(
          "test-projects root folder " + file.getAbsolutePath() + " not a folder or is unreadable");
    }
    return file;
  }
}
