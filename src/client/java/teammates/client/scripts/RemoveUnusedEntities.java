package teammates.client.scripts;

import java.util.List;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.common.collect.Lists;

/**
 * Deletes unused entities. These entities are either the ones used internally by legacy GAE
 * or the ones used by the system long ago but has since been deprecated.
 */
public class RemoveUnusedEntities {

    // To determine the entity kind to be deleted, comment out everything else except for that entity
    private static final String[] KIND_NAMES = {
            // GAE internal entities, no longer relevant since V8
            "_ah_SESSION",
            "_AE_Backup_Information",
            "_AE_Backup_Information_Kind_Files",
            "_AE_Backup_Information_Kind_Type_Info",
            "_AE_DatastoreAdmin_Operation",
            "_AE_TokenStorage_",
            "_GAE_MR_MapreduceControl",
            "_GAE_MR_MapreduceState",
            "_GAE_MR_ShardState",
            "_GAE_MR_TaskPayload",

            // Not sure where these come from?
            "TeamProfile",
            "TeamFormingSession",
            "TeamFormingLog",

            // Old, deprecated entities
            "AdminEmail", // never used
            "Evaluation", // deprecated in 2015
            "Submission", // deprecated in 2015
            "Student", // deprecated in 2016
            "Comment", // deprecated in 2017
    };

    public static void main(String... args) throws Exception {
        Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
        Query<Key> query = Query.newKeyQueryBuilder()
                .setKind(KIND_NAMES[0])
                .setLimit(1000)
                .build();
        while (true) {
            QueryResults<Key> keyQueryResults = datastore.run(query);
            List<Key> keys = Lists.newArrayList(keyQueryResults);

            if (keys.isEmpty()) {
                break;
            }

            datastore.delete(keys.toArray(new Key[0]));
        }
    }

}
