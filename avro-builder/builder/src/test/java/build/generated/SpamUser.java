package build.generated;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;






public class SpamUser extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 847958735356122484L;
  public static final org.apache.avro.Schema SCHEMA$ = AvroCompatibilityHelper.parse("{\"type\":\"record\",\"name\":\"SpamUser\",\"namespace\":\"build.generated\",\"fields\":[{\"name\":\"f\",\"type\":{\"type\":\"enum\",\"name\":\"SpamType\",\"doc\":\"Enum representing the different types of spam we can classify. BAM owns this definition, it is in common so that it can be included in both messages and tracking events.  Refer to https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/BAM+SpamType for the symbol definitions\",\"symbols\":[\"ADULT\",\"JOB\",\"EVENT\",\"UCV_SPAMMER\",\"PROMOTION\",\"OTHER_SPAM\",\"BADURL_MALWARE\",\"BADURL_PHISHING\",\"BADURL_SPAM\",\"NEW_MEMBER_LIX_BLOCKED\",\"BULK_SPAM\",\"PROFANITY\",\"PORN\",\"DUPLICATES\",\"COMMENT_META\",\"UBIQUITY_SPAM\",\"UBIQUITY_LOWQUALITY\",\"USER_FLAGGED_SPAM\",\"TOO_MANY_CAPS\",\"LANGUAGE_MISMATCH\",\"BAD_MEDIA\",\"MONEY_FRAUD\",\"TOS_VIOLATION\",\"COMMERCIAL\",\"SPAM_HASHES\",\"THREATS_OF_VIOLENCE\",\"IRRELEVANT\",\"GRATUITOUSLY_SHOCKING\",\"HARASSMENT\",\"IMPERSONATION\",\"PUZZLE\",\"MEME\",\"MEME_QUOTE_WITH_AUTHOR\",\"MEME_IRRELEVANT\",\"ADULT_NUDE_ART\",\"ADULT_BIKINI_LINGERIE\",\"ADULT_MEDICAL\",\"ADULT_IRRELEVANT\",\"SHOCKING\",\"SHOCKING_GLOBAL_EVENT\",\"SHOCKING_PERSONAL\",\"OFFENSIVE\",\"OFFENSIVE_PROFANITY\",\"PERSONAL\",\"PERSONAL_RELIGIOUS_BELIEF\",\"PERSONAL_POLITICAL_BELIEF\",\"PERSONAL_PHOTO\",\"PERSONAL_PLEA\",\"PERSONAL_SELF_PROMOTION\",\"PERSONAL_NON_PAID_FOR_JOB_POSTING\",\"OTHER_LOW_QUALITY\",\"MALWARE\",\"PHISHING\",\"GORE\",\"HATE_SPEECH\",\"PERSONAL_VIDEO\",\"AUDIO_LOW_QUALITY\",\"MUSIC_VIDEO\",\"MOVIE_VIDEO\",\"AUDIO_SPAM\",\"POTENTIAL_CHILD_PORN\",\"COPYRIGHT_VIOLATION\",\"COPYRIGHT_VIOLATION_AUDIO\",\"COPYRIGHT_VIOLATION_VIDEO\",\"FAKE_NEWS\",\"LOW_QUALITY_NEWS\",\"UNWANTED_ADVANCES\",\"TERRORISM\",\"EXTREME_VIOLENCE\",\"DEROGATORY\",\"VIOLATION_OF_HUMAN_DIGNITY\",\"GENOCIDE_DENIAL\",\"INFLAMMATORY_CONTENT\",\"INCITEMENT_OF_VIOLENCE\",\"HARASSMENT_SEXUAL\",\"HARASSMENT_NONSEXUAL\",\"EGREGIOUS_HARASSMENT_SEXUAL\",\"EGREGIOUS_HARASSMENT_NONSEXUAL\",\"DISCRIMINATION\",\"MISREPRESENTATION\",\"GREY_ZONE_CONTENT\",\"RED_ZONE_AUTHOR_CONTENT\",\"AUSTRIAN_LAW_VIOLATION\",\"ATO_SESSION_GENERATED_CONTENT\",\"MISINFORMATION_DISPUTED_HARM\",\"ILLEGAL_REGULATED_COMMERCIAL\",\"MISLEADING_CTA\",\"GRAPHIC_CONTENT\",\"GRAPHIC_NE\",\"GRAPHIC_NE_LQ\",\"MONEY_SCAM_DECEPTIVE\",\"MONEY_SCAM_WFH\",\"MONEY_SCAM_UPFRONT_PAYMENT\",\"MONEY_SCAM_IMPERSONATION\",\"MONEY_SCAM_PHISHING\",\"INFERIOR_INCOMPLETE\",\"INFERIOR_MULTIPLE_LISTINGS\",\"INFERIOR_BROKEN_LINK\",\"INFERIOR_COMPANY_MISMATCH\",\"INFERIOR_MISLEADING_POSITION\",\"INFERIOR_JOB_ALREADY_CLOSED\",\"DRUGS\",\"ALCOHOL\",\"GAMBLING\",\"FRANCHISES\",\"MLM\",\"WEAPONS\",\"CRYPTO_CURRENCY\",\"PROMOTION_SALE_PRODUCT_SERVICE\",\"PROMOTION_SELF_PROMOTION\",\"PROMOTION_TRAINING_COURSES\",\"DISCRIMINATION_RACE\",\"DISCRIMINATION_AGE\",\"DISCRIMINATION_SEX\",\"DISCRIMINATION_NATIONALITY\",\"DISCRIMINATION_MARITAL_STATUS\",\"DISCRIMINATION_MILITARY\",\"PROHIBITED_TRAFFICKING\",\"DISCRIMINATION_OTHER\",\"INFERIOR_NAME_FIELD_VIOLATION\",\"INFERIOR_OTHER\",\"INFERIOR_TEST_CONTENT\",\"MONEY_SCAM_ATO\",\"MONEY_SCAM_OTHER\",\"PROMOTION_OTHER\",\"NEAR\",\"HUMAN_BODY\",\"FAKE_ENGAGEMENT\",\"ALCOHOL_TOBACCO\",\"CIRCUMVENTION\",\"ANIMALS\",\"PHARMA\",\"MOB\",\"FABRICATED\",\"COUNTERFEIT\",\"PRECIOUS\",\"OCCULT\",\"HUMAN_EXPLOIT\",\"ILLEGAL_DRUG\",\"MONETIZATION_CSE\",\"RECREATIONAL_DRUG\",\"LOTTERY\",\"SPAM_TYPE_MIGRATION\",\"INAUTHENTIC_UNORIGINAL_CONTENT\",\"UNKNOWN\"],\"default\":\"UNKNOWN\"}}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static final org.apache.avro.specific.SpecificData MODEL$ = SpecificData.get();

  

   public build.generated.SpamType f;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public SpamUser() {}

  /**
   * All-args constructor.
   * @param f The new value for f
   */
  public SpamUser(build.generated.SpamType f) {
    this.f = f;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return f;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: f = (build.generated.SpamType)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'f' field.
   * @return The value of the 'f' field.
   */
  public build.generated.SpamType getF() {
    return f;
  }


  /**
   * Sets the value of the 'f' field.
   * @param value the value to set.
   */
  public void setF(build.generated.SpamType value) {
    this.f = value;
  }

  

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<SpamUser>
    WRITER$ = new org.apache.avro.specific.SpecificDatumWriter<>(SCHEMA$);

  public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, AvroCompatibilityHelper.newBinaryEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<SpamUser>
    READER$ = new org.apache.avro.specific.SpecificDatumReader<>(SCHEMA$);

  public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, AvroCompatibilityHelper.newBinaryDecoder(in));
  }

  
}










