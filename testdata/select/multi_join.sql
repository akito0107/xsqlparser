select count(*) from project
    join scenario ON scenario.project_id = project.project_id
    right outer join scenario_version ON scenario_version.scenario_id = scenario.scenario_id
    left outer join compare_log ON compare_log.left_scenario_version_id = scenario_version.scenario_version_id
    full outer join scenario_version scenario_version2 ON scenario_version2.scenario_version_id = compare_log.right_scenario_version_id
    inner join snapshot ON snapshot.scenario_version_id = scenario_version2.scenario_version_id
    natural join scenario_version as s3;