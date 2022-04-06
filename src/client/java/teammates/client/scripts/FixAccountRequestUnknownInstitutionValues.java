package teammates.client.scripts;

import java.util.HashMap;
import java.util.Map;

import com.googlecode.objectify.cmd.Query;

import teammates.common.util.Const;
import teammates.storage.entity.AccountRequest;

/**
 * Fixes existing account requests whose institution is "Unknown Institution".
 */
public class FixAccountRequestUnknownInstitutionValues extends DataMigrationEntitiesBaseScript<AccountRequest> {

    private static final Map<String, String> EMAIL_TO_INSTITUTE_MAPPING = new HashMap<>();

    static {
        // Add the values here
        EMAIL_TO_INSTITUTE_MAPPING.put("", "");
    }

    public static void main(String[] args) {
        new FixAccountRequestUnknownInstitutionValues().doOperationRemotely();
    }

    @Override
    protected Query<AccountRequest> getFilterQuery() {
        return ofy().load().type(AccountRequest.class)
                .filter("institute =", Const.UNKNOWN_INSTITUTION);
    }

    @Override
    protected boolean isPreview() {
        return true;
    }

    @Override
    protected boolean isMigrationNeeded(AccountRequest accountRequest) {
        if (accountRequest.getRegisteredAt() == null) {
            return false;
        }
        if (!EMAIL_TO_INSTITUTE_MAPPING.containsKey(accountRequest.getEmail())) {
            System.out.printf("Warning: no institute assigned for the email %s%n", accountRequest.getEmail());
        }
        return true;
    }

    @Override
    protected void migrateEntity(AccountRequest accountRequest) throws Exception {
        String newInstitute = EMAIL_TO_INSTITUTE_MAPPING.getOrDefault(accountRequest.getEmail(), Const.UNKNOWN_INSTITUTION);
        accountRequest.setInstitute(newInstitute);
        saveEntityDeferred(accountRequest);
    }

}
