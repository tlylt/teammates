package teammates.client.scripts;

import java.util.HashMap;
import java.util.Map;

import com.googlecode.objectify.cmd.Query;

import teammates.common.util.Const;
import teammates.storage.entity.Course;

/**
 * Fixes existing courses whose institution is "Unknown Institution".
 */
public class FixCourseUnknownInstitutionValues extends DataMigrationEntitiesBaseScript<Course> {

    private static final Map<String, String> COURSE_TO_INSTITUTE_MAPPING = new HashMap<>();

    static {
        // Add the values here
        COURSE_TO_INSTITUTE_MAPPING.put("", "");
    }

    public static void main(String[] args) {
        new FixCourseUnknownInstitutionValues().doOperationRemotely();
    }

    @Override
    protected Query<Course> getFilterQuery() {
        return ofy().load().type(Course.class)
                .filter("institute =", Const.UNKNOWN_INSTITUTION);
    }

    @Override
    protected boolean isPreview() {
        return true;
    }

    @Override
    protected boolean isMigrationNeeded(Course course) {
        return COURSE_TO_INSTITUTE_MAPPING.containsKey(course.getUniqueId());
    }

    @Override
    protected void migrateEntity(Course course) throws Exception {
        String newInstitute = COURSE_TO_INSTITUTE_MAPPING.getOrDefault(course.getInstitute(), Const.UNKNOWN_INSTITUTION);
        course.setInstitute(newInstitute);
        saveEntityDeferred(course);
    }

}
