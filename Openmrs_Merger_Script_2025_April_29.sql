--Initial Setup

SET autocommit = 0; -- Disable autocommit for transaction control
SET FOREIGN_KEY_CHECKS = 0; -- Temporarily disable foreign key checks
START TRANSACTION;
USE openmrs;

-- SECTION 1: PERSON DATA MERGING --

-- This section handles merging of core person data including person, person_name, and person_address records
-- Step 1: Prepare environment by adding person_id2 column if it doesn't exist
-- This column will store the original person_id from the source database

SET @dbname = DATABASE();
SET @tablename = 'person';
SET @columnname = 'person_id2';
SET @preparedStatement = (SELECT IF(
  (
    SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE
      (TABLE_SCHEMA = @dbname)
      AND (TABLE_NAME = @tablename)
      AND (COLUMN_NAME = @columnname)
  ) > 0,
  'SELECT 1', -- Column exists, do nothing
  CONCAT('ALTER TABLE ', @tablename, ' ADD COLUMN ', @columnname, ' INT NULL AFTER person_id')
));
PREPARE alterIfNotExists FROM @preparedStatement;
EXECUTE alterIfNotExists;
DEALLOCATE PREPARE alterIfNotExists;


-- Step 2: Create mapping table for person IDs
TRUNCATE openmrs.person_id_mapping;
CREATE TABLE IF NOT EXISTS person_id_mapping (
    old_person_id INT NOT NULL,
    new_person_id INT NOT NULL,
    PRIMARY KEY (old_person_id),
    INDEX (new_person_id)
) ENGINE=InnoDB;


-- Step 3: Merge person records with UUID conflict resolution
INSERT INTO person (
    person_id2,
    gender,
    birthdate,
    birthdate_estimated,
    dead,
    death_date,
    cause_of_death,
    creator,
    date_created,
    changed_by,
    date_changed,
    voided,
    voided_by,
    date_voided,
    void_reason,
    uuid,
    deathdate_estimated,
    birthtime,
    cause_of_death_non_coded
)
SELECT 
    p.person_id,
    p.gender,
    p.birthdate,
    p.birthdate_estimated,
    p.dead,
    p.death_date,
    p.cause_of_death,
    p.creator,
    p.date_created,
    p.changed_by,
    p.date_changed,
    p.voided,
    p.voided_by,
    p.date_voided,
    p.void_reason,
    -- Handle UUID conflicts by generating new UUIDs
    CASE 
        WHEN EXISTS (SELECT 1 FROM person WHERE uuid = p.uuid) THEN UUID()
        ELSE p.uuid
    END AS uuid,
    p.deathdate_estimated,
    p.birthtime,
    p.cause_of_death_non_coded
FROM awendodice.person p
WHERE p.voided = 0;

-- Step 4: Populate the person_id_mapping table
INSERT INTO person_id_mapping (old_person_id, new_person_id)
SELECT 
    p2.person_id,
    p1.person_id
FROM awendodice.person p2
JOIN person p1 ON p1.person_id2 = p2.person_id
ON DUPLICATE KEY UPDATE new_person_id = VALUES(new_person_id);

-- Step 5: Merge related tables (person_name)
INSERT INTO person_name (
    person_id,
    preferred,
    prefix,
    given_name,
    middle_name,
    family_name_prefix,
    family_name,
    family_name2,
    family_name_suffix,
    degree,
    creator,
    date_created,
    voided,
    voided_by,
    date_voided,
    void_reason,
    uuid
)
SELECT 
    m.new_person_id,
    n.preferred,
    n.prefix,
    n.given_name,
    n.middle_name,
    n.family_name_prefix,
    n.family_name,
    n.family_name2,
    n.family_name_suffix,
    n.degree,
    n.creator,
    n.date_created,
    n.voided,
    n.voided_by,
    n.date_voided,
    n.void_reason,
    CASE 
        WHEN EXISTS (SELECT 1 FROM person_name WHERE uuid = n.uuid) THEN UUID()
        ELSE n.uuid
    END AS uuid
FROM awendodice.person_name n
JOIN person_id_mapping m ON n.person_id = m.old_person_id
WHERE n.voided = 0;

-- Step 6: Merge person_address records with comprehensive conflict resolution
INSERT INTO person_address (
    person_id,
    preferred,
    address1,
    address2,
    city_village,
    state_province,
    postal_code,
    country,
    latitude,
    longitude,
    start_date,
    end_date,
    creator,
    date_created,
    voided,
    voided_by,
    date_voided,
    void_reason,
    county_district,
    address3,
    address4,
    address5,
    address6,
    date_changed,
    changed_by,
    uuid,
    address7,
    address8,
    address9,
    address10,
    address11,
    address12,
    address13,
    address14,
    address15
)
SELECT 
    m.new_person_id,
    a.preferred,
    a.address1,
    a.address2,
    a.city_village,
    a.state_province,
    a.postal_code,
    a.country,
    a.latitude,
    a.longitude,
    a.start_date,
    a.end_date,
    a.creator,
    a.date_created,
    a.voided,
    a.voided_by,
    a.date_voided,
    a.void_reason,
    a.county_district,
    a.address3,
    a.address4,
    a.address5,
    a.address6,
    a.date_changed,
    a.changed_by,
    -- Handle UUID conflicts and potential duplicate addresses
    CASE 
        WHEN EXISTS (SELECT 1 FROM person_address WHERE uuid = a.uuid) THEN UUID()
        WHEN EXISTS (
            SELECT 1 FROM person_address pa 
            WHERE pa.person_id = m.new_person_id
            AND (
                (pa.address1 = a.address1 OR (pa.address1 IS NULL AND a.address1 IS NULL))
                AND (pa.city_village = a.city_village OR (pa.city_village IS NULL AND a.city_village IS NULL))
                AND (pa.postal_code = a.postal_code OR (pa.postal_code IS NULL AND a.postal_code IS NULL))
            )
        ) THEN UUID()
        ELSE a.uuid
    END AS uuid,
    a.address7,
    a.address8,
    a.address9,
    a.address10,
    a.address11,
    a.address12,
    a.address13,
    a.address14,
    a.address15
FROM awendodice.person_address a
JOIN person_id_mapping m ON a.person_id = m.old_person_id
WHERE a.voided = 0;

-- Capture merge statistics
SELECT COUNT(*) INTO @records_merged FROM awendodice.person_address WHERE voided = 0;
SELECT ROW_COUNT() INTO @conflicts_resolved;

-- Verify and report merge results


SELECT 
    @records_merged AS 'Total address records attempted',
    @conflicts_resolved AS 'Address records with conflicts resolved',
    (SELECT COUNT(*) FROM person_address WHERE person_id IN 
        (SELECT new_person_id FROM person_id_mapping)) AS 'Total merged addresses',
    (SELECT COUNT(DISTINCT person_id) FROM person_address WHERE person_id IN 
        (SELECT new_person_id FROM person_id_mapping)) AS 'Persons with addresses merged';

-- Check for potential preferred address conflicts
SELECT 
    m.new_person_id,
    COUNT(CASE WHEN a.preferred = 1 THEN 1 END) AS 'Preferred addresses'
FROM person_id_mapping m
JOIN person_address a ON a.person_id = m.new_person_id
GROUP BY m.new_person_id
HAVING COUNT(CASE WHEN a.preferred = 1 THEN 1 END) > 1;

ANALYZE TABLE person, person_name, person_id_mapping;

COMMIT;
SET FOREIGN_KEY_CHECKS = 1;
SET autocommit = 1;
SELECT 'Merging Persons,Person_names,Person_addresses completed successfully' AS result;

-- SECTION 2: USERS DATA MIGRATION
-- This section handles merging of user accounts and their roles

SET autocommit = 0;
SET FOREIGN_KEY_CHECKS = 0;
START TRANSACTION;
use openmrs;
	DROP TABLE IF EXISTS  openmrs.merge_tracking;
    DROP TABLE IF EXISTS  openmrs.merge_tracking_roles;

    -- Create persistent tracking table instead of temporary
    -- Create tracking tables (persistent for cross-table operations)

    CREATE TABLE IF NOT EXISTS openmrs.merge_tracking (
        system_id INT PRIMARY KEY,
        username VARCHAR(50),
        new_user_id INT,
        merge_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB;
    
	CREATE TABLE IF NOT EXISTS openmrs.merge_tracking_roles (
			original_user_id INT,
			role VARCHAR(50),
			new_user_id INT,
			PRIMARY KEY (original_user_id, role)
		) ENGINE=InnoDB;

     -- Merge non-conflicting users
    INSERT INTO openmrs.users (
        system_id, username, password, salt, secret_question, secret_answer,
        creator, date_created, changed_by, date_changed, person_id,
        retired, retired_by, date_retired, retire_reason, uuid,
        activation_key, email
    )
    SELECT 
        u2.system_id, u2.username, u2.password, u2.salt, u2.secret_question,
        u2.secret_answer, u2.creator, u2.date_created, u2.changed_by,
        u2.date_changed, m.new_person_id, u2.retired, u2.retired_by,
        u2.date_retired, u2.retire_reason, u2.uuid, u2.activation_key, u2.email
    FROM 
        awendodice.users u2
	JOIN person_id_mapping m ON u2.person_id = m.old_person_id
    WHERE u2.retired=0 and u2.system_id NOT IN ('admin','daemon');
    
 --    -- Tracking merged users
 INSERT INTO openmrs.merge_tracking (system_id, username, new_user_id)
 SELECT u2.system_id, u2.username, u1.user_id
 FROM awendodice.users u2
 JOIN person_id_mapping m ON u2.person_id = m.old_person_id
 JOIN openmrs.users u1 ON m.new_person_id=u1.person_id 
 WHERE u2.retired=0 and u2.system_id NOT IN ('admin','daemon');
 
        
	-- Prepare user roles mapping
		INSERT IGNORE INTO openmrs.merge_tracking_roles (original_user_id, role, new_user_id)
		SELECT 
			ur.user_id, 
			ur.role, 
			mt.new_user_id
		FROM awendodice.user_role ur
		JOIN awendodice.users u ON ur.user_id = u.user_id
		JOIN openmrs.merge_tracking mt ON u.system_id = mt.system_id
		WHERE NOT EXISTS (
			SELECT 1 FROM openmrs.merge_tracking_roles mtr 
			WHERE mtr.original_user_id = ur.user_id AND mtr.role = ur.role
		);
		-- Merge user roles
		INSERT INTO openmrs.user_role (user_id, role)
		SELECT 
			new_user_id, 
			role
		FROM 
			openmrs.merge_tracking_roles;
    -- Verification of users and roles record migrated
    SELECT 
        (SELECT COUNT(*) FROM awendodice.users) AS source_users,
        (SELECT COUNT(*) FROM openmrs.merge_tracking) AS merged_users,
        (SELECT COUNT(*) FROM awendodice.user_role) AS source_roles,
        (SELECT COUNT(*) FROM openmrs.merge_tracking_roles) AS merged_roles,
        (SELECT COUNT(*) FROM openmrs.user_role WHERE user_id IN 
		(SELECT new_user_id FROM openmrs.merge_tracking)) AS roles_in_target;

 -- Reinstate the FK values

    COMMIT;
    SET FOREIGN_KEY_CHECKS = 1;
    SET autocommit = 1;
    SELECT 'Merge users, users_role records completed successfully' AS result;
    -- Users Merging Ends Here


    -- SECTION 3 PROVIDERS RECORD MERGING COMMENCE HERE!!
    -- Handle the FK related to the providers tables by setting them 0

    SET autocommit = 0;
	SET FOREIGN_KEY_CHECKS = 0;
	START TRANSACTION;
	USE openmrs;

    DROP TABLE IF EXISTS  openmrs.temp_provider_mapping;
    -- Create mapping table for provider IDs
    CREATE TABLE IF NOT EXISTS temp_provider_mapping (
        source_provider_id INT PRIMARY KEY,
        target_provider_id INT,
        source_uuid CHAR(38),
        merge_status VARCHAR(20)
    ) ENGINE=InnoDB;

    -- Merge providers by UUID first (most reliable)
    INSERT IGNORE INTO openmrs.provider (
        person_id, name, identifier, creator, date_created,
        changed_by, date_changed, retired, retired_by,
        date_retired, retire_reason, uuid, role_id, speciality_id
    )
    SELECT 
        per.person_id, -- Mapped from your previous person merge
        p.name,p.identifier,COALESCE(p.creator, 1) AS creator, -- Default to admin if not mapped
        p.date_created,p.changed_by,p.date_changed,p.retired,p.retired_by,p.date_retired,
        p.retire_reason,p.uuid,p.role_id,p.speciality_id
    FROM 
        awendodice.provider p
    LEFT JOIN 
        openmrs.person per ON p.person_id = per.person_id2 -- Assuming your person mapping uses person_id2
    WHERE retired=0;
        
    -- Track UUID-based merges
    INSERT INTO temp_provider_mapping (source_provider_id, target_provider_id, source_uuid, merge_status)
    SELECT 
        p.provider_id,
        new_p.provider_id,
        p.uuid,
        'merged_by_uuid'
    FROM 
        awendodice.provider p
    JOIN 
        openmrs.provider new_p ON p.uuid = new_p.uuid;

   
    -- Merge provider attributes
    INSERT IGNORE INTO openmrs.provider_attribute (
        provider_id, attribute_type_id, value_reference, uuid,
        creator, date_created, changed_by, date_changed,
        voided, voided_by, date_voided, void_reason
    )
    SELECT 
        pm.target_provider_id,
        pa.attribute_type_id,
        pa.value_reference,
        pa.uuid,
        COALESCE(pa.creator, 1) AS creator,
        pa.date_created,
        pa.changed_by,
        pa.date_changed,
        pa.voided,
        pa.voided_by,
        pa.date_voided,
        pa.void_reason
    FROM 
        awendodice.provider_attribute pa
    JOIN 
        temp_provider_mapping pm ON pa.provider_id = pm.source_provider_id
    WHERE voided=0;
    
    -- Verification of migrated records
    SELECT 
        (SELECT COUNT(*) FROM awendodice.provider) AS source_providers,
        (SELECT COUNT(*) FROM temp_provider_mapping) AS merged_providers,
        (SELECT COUNT(*) FROM awendodice.provider p
         WHERE NOT EXISTS (
             SELECT 1 FROM temp_provider_mapping 
             WHERE source_provider_id = p.provider_id
         )) AS unmerged_providers,
        (SELECT COUNT(*) FROM awendodice.provider_attribute) AS source_attributes,
        (SELECT COUNT(*) FROM openmrs.provider_attribute pa
         JOIN temp_provider_mapping pm ON pa.provider_id = pm.target_provider_id) AS merged_attributes;

    COMMIT;
    SET FOREIGN_KEY_CHECKS = 1;
    SET autocommit = 1;
    
    SELECT 'Provider merge completed successfully' AS result;
       
	SELECT 'PATIENT merge started successfully' AS result;
    -- END SECTION  FOR PROVIDERS RECORDS

    -- SECTION FOR PATIENT RECORD MERGING PROCESS STARTS HERE!!
    
    SET autocommit = 0;
	SET FOREIGN_KEY_CHECKS = 0;
	START TRANSACTION;
    USE openmrs;

    DROP TABLE IF EXISTS  openmrs.merge_tracking_patients;
    -- Create tracking table for merged patients
    CREATE TABLE IF NOT EXISTS openmrs.merge_tracking_patients (
        source_patient_id INT PRIMARY KEY,
        target_patient_id INT,
        merge_status VARCHAR(20),
        merge_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB;
    
    -- Create accurate mapping table between source and target patient IDs
    DROP TABLE IF EXISTS  openmrs.temp_patient_id_mapping;
    CREATE TABLE IF NOT EXISTS temp_patient_id_mapping(
        source_patient_id INT PRIMARY KEY,
        target_patient_id INT UNIQUE,
        merge_status VARCHAR(20)
    ) ENGINE=InnoDB;

    -- Populate the mapping table with correct ID relationships
    INSERT IGNORE INTO openmrs.temp_patient_id_mapping (source_patient_id, target_patient_id, merge_status)
    SELECT 
        p.patient_id,
        per.person_id,  -- Note: Using person_id NOT person_id2
        'pending'
    FROM awendodice.patient p
    JOIN openmrs.person per ON p.patient_id = per.person_id2
    WHERE p.voided = 0;
    
    -- Merge patients that already exist in target person table
    INSERT INTO openmrs.patient (
        patient_id, creator, date_created, changed_by, date_changed,
        voided, voided_by, date_voided, void_reason, allergy_status
    )
        SELECT 
        map.target_patient_id,
        COALESCE(um.new_user_id, 1) as creator,
        p.date_created,
		p.changed_by,
        p.date_changed,
        p.voided,
        p.voided_by,
        p.date_voided,
        p.void_reason,
        p.allergy_status
    FROM awendodice.patient p
    JOIN temp_patient_id_mapping map ON p.patient_id = map.source_patient_id
   LEFT JOIN openmrs.merge_tracking um ON p.creator = um.new_user_id
    WHERE 
        NOT EXISTS (
            SELECT 1 FROM openmrs.patient 
            WHERE patient_id = map.target_patient_id
        )
    AND p.voided = 0;
    
     -- Update mapping table with merge status
    UPDATE temp_patient_id_mapping map
    SET merge_status = 'merged'
    WHERE EXISTS (
        SELECT 1 FROM openmrs.patient 
        WHERE patient_id = map.target_patient_id
    );
    
    COMMIT;
    SET FOREIGN_KEY_CHECKS = 1;
    SET autocommit = 1;
    
    SELECT 'Patient merge completed successfully' AS result;
    
    -- PATIENT_PROGRAM PATIENT_IDENTIFIER STARTS HERE
    SELECT 'PATIENT PROGRAM_PATIENT_IDENTIFIRS STARTED SUCCESSFULLY' AS result;
    
	SET autocommit = 0;
	SET FOREIGN_KEY_CHECKS = 0;
	START TRANSACTION;
	use openmrs;

    -- Create mapping tables for patient programs and identifiers
    DROP TABLE IF EXISTS  openmrs.temp_program_mapping;
    CREATE TABLE IF NOT EXISTS temp_program_mapping (
        source_program_id INT PRIMARY KEY,
        target_program_id INT,
        source_uuid CHAR(38),
        merge_status VARCHAR(20)
    ) ENGINE=InnoDB;
    
    DROP TABLE IF EXISTS  openmrs.temp_identifier_mapping;
    CREATE TABLE IF NOT EXISTS temp_identifier_mapping (
        source_identifier_id INT PRIMARY KEY,
        target_identifier_id INT,
        source_uuid CHAR(38),
        merge_status VARCHAR(20)
    ) ENGINE=InnoDB;

    -- Merge patient programs by UUID first
    INSERT IGNORE INTO openmrs.patient_program (
        patient_id, program_id, date_enrolled, date_completed,
        location_id, outcome_concept_id, creator, date_created,
        changed_by, date_changed, voided, voided_by,
        date_voided, void_reason, uuid
    )
    SELECT 
        pm.target_patient_id,
        pp.program_id,
        pp.date_enrolled,
        pp.date_completed,
        pp.location_id,
        pp.outcome_concept_id,
        COALESCE(pp.creator, 1) AS creator,
        pp.date_created,
        pp.changed_by,
        pp.date_changed,
        pp.voided,
        pp.voided_by,
        pp.date_voided,
        pp.void_reason,
        pp.uuid
    FROM 
        awendodice.patient_program pp
    JOIN 
        awendodice.patient p ON pp.patient_id = p.patient_id
    JOIN 
        temp_patient_id_mapping pm ON p.patient_id = pm.source_patient_id
    WHERE 
        NOT EXISTS (SELECT 1 FROM openmrs.patient_program WHERE uuid = pp.uuid);

    -- Track merged programs
    INSERT INTO temp_program_mapping (source_program_id, target_program_id, source_uuid, merge_status)
    SELECT 
        pp.patient_program_id,
        new_pp.patient_program_id,
        pp.uuid,
        'merged_by_uuid'
    FROM 
        awendodice.patient_program pp
    JOIN 
        openmrs.patient_program new_pp ON pp.uuid = new_pp.uuid;

    -- Merge patient identifiers by UUID
    INSERT IGNORE INTO openmrs.patient_identifier (
        patient_id, identifier, identifier_type, preferred,
        location_id, creator, date_created, date_changed,
        changed_by, voided, voided_by, date_voided,
        void_reason, uuid, patient_program_id
    )
    SELECT 
        pm.target_patient_id,
        pi.identifier,
        pi.identifier_type,
        pi.preferred,
        pi.location_id,
        COALESCE(pi.creator, 1) AS creator,
        pi.date_created,
        pi.date_changed,
        pi.changed_by,
        pi.voided,
        pi.voided_by,
        pi.date_voided,
        pi.void_reason,
        pi.uuid,
        ppm.target_program_id AS patient_program_id
    FROM 
        awendodice.patient_identifier pi
    JOIN 
        awendodice.patient p ON pi.patient_id = p.patient_id
    JOIN 
        temp_patient_id_mapping pm ON p.patient_id = pm.source_patient_id
    LEFT JOIN 
        awendodice.patient_program pp ON pi.patient_program_id = pp.patient_program_id
   LEFT JOIN 
       temp_program_mapping ppm ON pp.patient_program_id = ppm.source_program_id
    WHERE 
        NOT EXISTS (SELECT 1 FROM openmrs.patient_identifier WHERE uuid = pi.uuid);

    -- Track merged identifiers
    INSERT INTO temp_identifier_mapping (source_identifier_id, target_identifier_id, source_uuid, merge_status)
    SELECT 
        pi.patient_identifier_id,
        new_pi.patient_identifier_id,
        pi.uuid,
        'merged_by_uuid'
    FROM awendodice.patient_identifier pi
    JOIN openmrs.patient_identifier new_pi ON pi.uuid = new_pi.uuid;

    -- Verification
    SELECT 
        (SELECT COUNT(*) FROM awendodice.patient_program) AS source_programs,
        (SELECT COUNT(*) FROM temp_program_mapping) AS merged_programs,
        (SELECT COUNT(*) FROM awendodice.patient_identifier) AS source_identifiers,
        (SELECT COUNT(*) FROM temp_identifier_mapping) AS merged_identifiers;

    COMMIT;
    SET FOREIGN_KEY_CHECKS = 1;
    SET autocommit = 1;
    
    SELECT 'Patient programs and identifiers merge completed successfully' AS result;
    
    -- SECTION FOR PATIENT APPOINTMENT STARTS HERE!!!
    SELECT 'Patient APPOINTMENTS merge STARTED successfully' AS result;
    
    SET autocommit = 0;
	SET FOREIGN_KEY_CHECKS = 0;
	START TRANSACTION;
	use openmrs;
    -- Create mapping tables for appointments
    DROP TABLE IF EXISTS  openmrs.temp_appointment_mapping; 
    CREATE TABLE IF NOT EXISTS temp_appointment_mapping (
        source_appointment_id INT PRIMARY KEY,
        target_appointment_id INT,
        source_uuid VARCHAR(38),
        merge_status VARCHAR(20))
    ENGINE=InnoDB;

    -- Migrate appointments by UUID first (most reliable)
    INSERT IGNORE INTO openmrs.patient_appointment (
        provider_id, appointment_number, patient_id, start_date_time,
        end_date_time, appointment_service_id, appointment_service_type_id,
        status, location_id, appointment_kind, comments, uuid,
        date_created, creator, date_changed, changed_by, voided,
        voided_by, date_voided, void_reason, related_appointment_id,
        tele_health_video_link, date_honored, priority, date_appointment_scheduled
    )
    SELECT 
        pm.target_provider_id,
        pa.appointment_number,
        ptm.target_patient_id,
        pa.start_date_time,
        pa.end_date_time,
        pa.appointment_service_id,
        pa.appointment_service_type_id,
        pa.status,
        pa.location_id,
        pa.appointment_kind,
        pa.comments,
        pa.uuid,
        pa.date_created,
        COALESCE(pa.creator, 1) AS creator,
        pa.date_changed,
        pa.changed_by,
        pa.voided,
        pa.voided_by,
        pa.date_voided,
        pa.void_reason,
        NULL, -- Will update related appointments in Step 3
        pa.tele_health_video_link,
        NULL as date_honored,
        pa.priority,
        pa.date_appointment_scheduled
    FROM 
        awendodice.patient_appointment pa
    JOIN 
        awendodice.patient p ON pa.patient_id = p.patient_id
    JOIN 
        temp_patient_id_mapping ptm ON p.patient_id = ptm.source_patient_id
 LEFT JOIN 
     awendodice.provider pr ON pa.provider_id = pr.provider_id
    LEFT JOIN 
        temp_provider_mapping pm ON pa.provider_id = pm.source_provider_id
    WHERE 
        NOT EXISTS (SELECT 1 FROM openmrs.patient_appointment WHERE uuid = pa.uuid);

    -- Track migrated appointments
    INSERT INTO temp_appointment_mapping (source_appointment_id, target_appointment_id, source_uuid, merge_status)
    SELECT 
        pa.patient_appointment_id,
        new_pa.patient_appointment_id,
        pa.uuid,
        'migrated_by_uuid'
    FROM 
        awendodice.patient_appointment pa
    JOIN 
        openmrs.patient_appointment new_pa ON pa.uuid = new_pa.uuid
    WHERE 
        NOT EXISTS (SELECT 1 FROM temp_appointment_mapping WHERE source_appointment_id = pa.patient_appointment_id);

   
    -- Update related appointment references
    UPDATE openmrs.patient_appointment target
    JOIN temp_appointment_mapping tm ON target.patient_appointment_id = tm.target_appointment_id
    JOIN awendodice.patient_appointment source ON tm.source_appointment_id = source.patient_appointment_id
    LEFT JOIN temp_appointment_mapping related_tm ON source.related_appointment_id = related_tm.source_appointment_id
    SET target.related_appointment_id = related_tm.target_appointment_id
    WHERE source.related_appointment_id IS NOT NULL;

    -- Migrate appointment providers
    INSERT IGNORE INTO openmrs.patient_appointment_provider (
        patient_appointment_id, provider_id, response,
        comments, date_created, creator, date_changed,
        changed_by, voided, voided_by, date_voided,
        void_reason, uuid
    )
    SELECT 
        am.target_appointment_id,
        pm.target_provider_id,
        pap.response,
        pap.comments,
        pap.date_created,
        COALESCE(pap.creator, 1) AS creator,
        pap.date_changed,
        pap.changed_by,
        pap.voided,
        pap.voided_by,
        pap.date_voided,
        pap.void_reason,
        pap.uuid
    FROM 
        awendodice.patient_appointment_provider pap
    JOIN 
        awendodice.patient_appointment pa ON pap.patient_appointment_id = pa.patient_appointment_id
    JOIN 
        temp_appointment_mapping am ON pa.patient_appointment_id = am.source_appointment_id
    JOIN 
        awendodice.provider p ON pap.provider_id = p.provider_id
    LEFT JOIN 
        temp_provider_mapping pm ON p.provider_id = pm.source_provider_id
    WHERE 
        NOT EXISTS (SELECT 1 FROM openmrs.patient_appointment_provider WHERE uuid = pap.uuid);

    -- Migrate appointment audit logs
    INSERT IGNORE INTO openmrs.patient_appointment_audit (
        appointment_id, uuid, date_created, creator,
        date_changed, changed_by, voided, voided_by,
        date_voided, void_reason, status, notes
    )
    SELECT 
        am.target_appointment_id,
        paa.uuid,
        paa.date_created,
        COALESCE(paa.creator, 1) AS creator,
        paa.date_changed,
        paa.changed_by,
        paa.voided,
        paa.voided_by,
        paa.date_voided,
        paa.void_reason,
        paa.status,
        paa.notes
    FROM 
        awendodice.patient_appointment_audit paa
    JOIN 
        awendodice.patient_appointment pa ON paa.appointment_id = pa.patient_appointment_id
    JOIN 
        temp_appointment_mapping am ON pa.patient_appointment_id = am.source_appointment_id
    WHERE 
        NOT EXISTS (SELECT 1 FROM openmrs.patient_appointment_audit WHERE uuid = paa.uuid);

    -- Verification
    SELECT 
        (SELECT COUNT(*) FROM awendodice.patient_appointment) AS source_appointments,
        (SELECT COUNT(*) FROM temp_appointment_mapping) AS migrated_appointments,
        (SELECT COUNT(*) FROM awendodice.patient_appointment pa
         WHERE NOT EXISTS (
             SELECT 1 FROM temp_appointment_mapping 
             WHERE source_appointment_id = pa.patient_appointment_id
         )) AS unmigrated_appointments,
        (SELECT COUNT(*) FROM awendodice.patient_appointment_provider) AS source_providers,
        (SELECT COUNT(*) FROM openmrs.patient_appointment_provider pap
         JOIN temp_appointment_mapping am ON pap.patient_appointment_id = am.target_appointment_id) AS migrated_providers,
        (SELECT COUNT(*) FROM awendodice.patient_appointment_audit) AS source_audits,
        (SELECT COUNT(*) FROM openmrs.patient_appointment_audit paa
         JOIN temp_appointment_mapping am ON paa.appointment_id = am.target_appointment_id) AS migrated_audits;

    COMMIT;
    SET FOREIGN_KEY_CHECKS = 1;
    SET autocommit = 1;
    SELECT 'Patient appointment migration completed successfully' AS result;
    
    
--  VISIT RECORDS MIGRATION STARTS HERE!!
    SET autocommit = 0;
	SET FOREIGN_KEY_CHECKS = 0;
	START TRANSACTION;
	USE openmrs;


    -- Create mapping tables for visits
    DROP TABLE IF EXISTS  openmrs.temp_visit_mapping;
    CREATE TABLE IF NOT EXISTS temp_visit_mapping (
        source_visit_id INT PRIMARY KEY,
        target_visit_id INT,
        source_uuid CHAR(38),
        merge_status VARCHAR(20)
    ) ENGINE=InnoDB;

    -- Migrate visits by UUID first
    INSERT IGNORE INTO openmrs.visit (
        patient_id, visit_type_id, date_started, date_stopped,
        indication_concept_id, location_id, creator, date_created,
        changed_by, date_changed, voided, voided_by,
        date_voided, void_reason, uuid
    )
    SELECT 
        pm.target_patient_id,
        v.visit_type_id,
        v.date_started,
        v.date_stopped,
        v.indication_concept_id,
        v.location_id,
        COALESCE(v.creator, 1) AS creator,
        v.date_created,
        v.changed_by,
        v.date_changed,
        v.voided,
        v.voided_by,
        v.date_voided,
        v.void_reason,
        v.uuid
    FROM 
        awendodice.visit v
    JOIN 
        awendodice.patient p ON v.patient_id = p.patient_id
    JOIN 
        temp_patient_id_mapping pm ON p.patient_id = pm.source_patient_id
    WHERE 
        NOT EXISTS (SELECT 1 FROM openmrs.visit WHERE uuid = v.uuid);

    -- Track migrated visits
    INSERT INTO temp_visit_mapping (source_visit_id, target_visit_id, source_uuid, merge_status)
    SELECT 
        v.visit_id,
        new_v.visit_id,
        v.uuid,
        'migrated_by_uuid'
    FROM 
        awendodice.visit v
    JOIN 
        openmrs.visit new_v ON v.uuid = new_v.uuid
    WHERE 
        NOT EXISTS (SELECT 1 FROM temp_visit_mapping WHERE source_visit_id = v.visit_id);


    -- Migrate visit attributes by UUID
    INSERT IGNORE INTO openmrs.visit_attribute (
        visit_id, attribute_type_id, value_reference, uuid,
        creator, date_created, changed_by, date_changed,
        voided, voided_by, date_voided, void_reason
    )
    SELECT 
        vm.target_visit_id,
        va.attribute_type_id,
        va.value_reference,
        va.uuid,
        COALESCE(va.creator, 1) AS creator,
        va.date_created,
        va.changed_by,
        va.date_changed,
        va.voided,
        va.voided_by,
        va.date_voided,
        va.void_reason
    FROM 
        awendodice.visit_attribute va
    JOIN 
        awendodice.visit v ON va.visit_id = v.visit_id
    JOIN 
        temp_visit_mapping vm ON v.visit_id = vm.source_visit_id
    WHERE 
        NOT EXISTS (SELECT 1 FROM openmrs.visit_attribute WHERE uuid = va.uuid);

    -- Verification
    SELECT 
        (SELECT COUNT(*) FROM awendodice.visit) AS source_visits,
        (SELECT COUNT(*) FROM temp_visit_mapping) AS migrated_visits,
        (SELECT COUNT(*) FROM awendodice.visit v
         WHERE NOT EXISTS (
             SELECT 1 FROM temp_visit_mapping 
             WHERE source_visit_id = v.visit_id
         )) AS unmigrated_visits,
        (SELECT COUNT(*) FROM awendodice.visit_attribute) AS source_attributes,
        (SELECT COUNT(*) FROM openmrs.visit_attribute va
         JOIN temp_visit_mapping vm ON va.visit_id = vm.target_visit_id) AS migrated_attributes;

    COMMIT;
    SET FOREIGN_KEY_CHECKS = 1;
    SET autocommit = 1;
    SELECT 'Visit and visit attribute migration completed successfully' AS result;

-- ENCOUNTER RECORDS MERGING STARTS HERE
	SELECT 'ENCOUNTER RECORDS migration completed successfully' AS result;
	SET autocommit = 0;
	SET FOREIGN_KEY_CHECKS = 0;
	START TRANSACTION;
	use openmrs;
    -- Create mapping tables for encounters
    DROP TABLE IF EXISTS  openmrs.temp_encounter_mapping;
    CREATE TABLE IF NOT EXISTS temp_encounter_mapping (
        source_encounter_id INT PRIMARY KEY,
        target_encounter_id INT,
        source_uuid CHAR(38),
        merge_status VARCHAR(20))
    ENGINE=InnoDB;

    --Migrate encounters by UUID first (most reliable)
    INSERT IGNORE INTO openmrs.encounter (
        encounter_type, patient_id, location_id, form_id,
        encounter_datetime, creator, date_created, voided,
        voided_by, date_voided, void_reason, changed_by,
        date_changed, visit_id, uuid
    )
    SELECT 
        e.encounter_type,
        pm.target_patient_id,
        e.location_id,
        e.form_id,
        e.encounter_datetime,
        COALESCE(e.creator, 1) AS creator,
        e.date_created,
        e.voided,
        e.voided_by,
        e.date_voided,
        e.void_reason,
        e.changed_by,
        e.date_changed,
        vm.target_visit_id AS visit_id,
        e.uuid
    FROM 
        awendodice.encounter e
    JOIN 
        awendodice.patient p ON e.patient_id = p.patient_id
    JOIN 
        temp_patient_id_mapping pm ON p.patient_id = pm.source_patient_id
    LEFT JOIN 
        awendodice.visit v ON e.visit_id = v.visit_id
    LEFT JOIN 
        temp_visit_mapping vm ON v.visit_id = vm.source_visit_id
    WHERE 
        NOT EXISTS (SELECT 1 FROM openmrs.encounter WHERE uuid = e.uuid);

    -- Track migrated encounters
    INSERT INTO temp_encounter_mapping (source_encounter_id, target_encounter_id, source_uuid, merge_status)
    SELECT 
        e.encounter_id,
        new_e.encounter_id,
        e.uuid,
        'migrated_by_uuid'
    FROM 
        awendodice.encounter e
    JOIN 
        openmrs.encounter new_e ON e.uuid = new_e.uuid
    WHERE 
        NOT EXISTS (SELECT 1 FROM temp_encounter_mapping WHERE source_encounter_id = e.encounter_id);
        
    -- Migrate encounter providers
    INSERT IGNORE INTO openmrs.encounter_provider (
        encounter_id, provider_id, encounter_role_id,
        creator, date_created, changed_by, date_changed,
        voided, date_voided, voided_by, void_reason, uuid
    )
    SELECT 
        em.target_encounter_id,
        pm.target_provider_id,
        ep.encounter_role_id,
        COALESCE(ep.creator, 1) AS creator,
        ep.date_created,
        ep.changed_by,
        ep.date_changed,
        ep.voided,
        ep.date_voided,
        ep.voided_by,
        ep.void_reason,
        ep.uuid
    FROM 
        awendodice.encounter_provider ep
    JOIN 
        awendodice.encounter e ON ep.encounter_id = e.encounter_id
    JOIN 
        temp_encounter_mapping em ON e.encounter_id = em.source_encounter_id
    JOIN 
        awendodice.provider p ON ep.provider_id = p.provider_id
    LEFT JOIN 
        temp_provider_mapping pm ON p.provider_id = pm.source_provider_id
    WHERE 
        NOT EXISTS (SELECT 1 FROM openmrs.encounter_provider WHERE uuid = ep.uuid);

    -- Verification
    SELECT 
        (SELECT COUNT(*) FROM awendodice.encounter) AS source_encounters,
        (SELECT COUNT(*) FROM temp_encounter_mapping) AS migrated_encounters,
        (SELECT COUNT(*) FROM awendodice.encounter e
         WHERE NOT EXISTS (
             SELECT 1 FROM temp_encounter_mapping 
             WHERE source_encounter_id = e.encounter_id
         )) AS unmigrated_encounters,
        (SELECT COUNT(*) FROM awendodice.encounter_provider) AS source_providers,
        (SELECT COUNT(*) FROM openmrs.encounter_provider ep
         JOIN temp_encounter_mapping em ON ep.encounter_id = em.target_encounter_id) AS migrated_providers;
    COMMIT;
    SET FOREIGN_KEY_CHECKS = 1;
    SET autocommit = 1;
    SELECT 'Encounter and encounter provider migration completed successfully' AS result;
    
    
    -- ORDERS AND OBSERVATION RECORDS MERGING NOW
	SELECT 'ORDERS and OBS migration completed successfully' AS result; 
    SET autocommit = 0;
	SET FOREIGN_KEY_CHECKS = 0;
	START TRANSACTION;
	use openmrs;
    
    -- Create mapping tables for observations and orders
    DROP TABLE IF EXISTS  openmrs.temp_obs_mapping;
    CREATE TABLE IF NOT EXISTS temp_obs_mapping (
        source_obs_id INT PRIMARY KEY,
        target_obs_id INT,
        source_uuid CHAR(38),
        merge_status VARCHAR(20))
    ENGINE=InnoDB;
    
    DROP TABLE IF EXISTS  openmrs.temp_order_mapping;
    CREATE TABLE IF NOT EXISTS temp_order_mapping (
        source_order_id INT PRIMARY KEY,
        target_order_id INT,
        source_uuid CHAR(38),
        merge_status VARCHAR(20))
    ENGINE=InnoDB;
    	  
      -- Create order_group mapping table if not exists
DROP TABLE IF EXISTS openmrs.temp_order_group_mapping; 
CREATE TABLE IF NOT EXISTS openmrs.temp_order_group_mapping (
    source_order_group_id INT PRIMARY KEY,
    target_order_group_id INT,
    source_uuid CHAR(38),
    merge_status VARCHAR(20)
) ENGINE=InnoDB;

    -- Migrate orders first (since obs references orders)
    INSERT IGNORE INTO openmrs.orders (
        order_type_id, concept_id, orderer, encounter_id,
        instructions, date_activated, auto_expire_date, date_stopped,
        order_reason, order_reason_non_coded, creator, date_created,
        voided, voided_by, date_voided, void_reason, patient_id,
        accession_number, uuid, urgency, order_number, previous_order_id,
        order_action, comment_to_fulfiller, care_setting, scheduled_date,
        order_group_id, sort_weight, fulfiller_comment, fulfiller_status,
        form_namespace_and_path
    )
    SELECT 
        o.order_type_id,
        o.concept_id,
        pm.target_provider_id AS orderer,
        em.target_encounter_id AS encounter_id,
        o.instructions,
        o.date_activated,
        o.auto_expire_date,
        o.date_stopped,
        o.order_reason,
        o.order_reason_non_coded,
        COALESCE(o.creator, 1) AS creator,
        o.date_created,
        o.voided,
        o.voided_by,
        o.date_voided,
        o.void_reason,
        ptm.target_patient_id AS patient_id,
        o.accession_number,
        o.uuid,
        o.urgency,
        o.order_number,
        NULL, -- Will update previous_order_id in Step 3
        o.order_action,
        o.comment_to_fulfiller,
        o.care_setting,
        o.scheduled_date,
        NULL as order_group_id, -- Will update order_group_id if needed
        o.sort_weight,
        o.fulfiller_comment,
        o.fulfiller_status,
        o.form_namespace_and_path
    FROM 
        awendodice.orders o
    JOIN 
        awendodice.patient p ON o.patient_id = p.patient_id
    JOIN 
        temp_patient_id_mapping ptm ON p.patient_id = ptm.source_patient_id
    JOIN 
        awendodice.encounter e ON o.encounter_id = e.encounter_id
    JOIN 
        temp_encounter_mapping em ON e.encounter_id = em.source_encounter_id
    LEFT JOIN 
        awendodice.provider pr ON o.orderer = pr.provider_id
    LEFT JOIN 
        temp_provider_mapping pm ON pr.provider_id = pm.source_provider_id
    WHERE 
        NOT EXISTS (SELECT 1 FROM openmrs.orders WHERE uuid = o.uuid);


    -- Track migrated orders
    INSERT INTO temp_order_mapping (source_order_id, target_order_id, source_uuid, merge_status)
    SELECT 
        o.order_id,
        new_o.order_id,
        o.uuid,
        'migrated_by_uuid'
    FROM 
        awendodice.orders o
    JOIN 
        openmrs.orders new_o ON o.uuid = new_o.uuid
    WHERE 
        NOT EXISTS (SELECT 1 FROM temp_order_mapping WHERE source_order_id = o.order_id);
        
      --  Migrate drug orders (only for migrated base orders)
	INSERT INTO openmrs.drug_order (
		order_id,
		drug_inventory_id,
		dose,
		as_needed,
		dosing_type,
		quantity,
		as_needed_condition,
		num_refills,
		dosing_instructions,
		duration,
		duration_units,
		quantity_units,
		route,
		dose_units,
		frequency,
		brand_name,
		dispense_as_written,
		drug_non_coded
	)
	SELECT 
		om.target_order_id AS order_id, -- Mapped order ID from temp_order_mapping
		do.drug_inventory_id, -- Assuming drug IDs are the same in both systems
		do.dose,
		do.as_needed,
		do.dosing_type,
		do.quantity,
		do.as_needed_condition,
		do.num_refills,
		do.dosing_instructions,
		do.duration,
		do.duration_units, -- Assuming concept IDs are the same
		do.quantity_units,
		do.route,
		do.dose_units,
		do.frequency,
		do.brand_name,
		do.dispense_as_written,
		do.drug_non_coded
	FROM awendodice.drug_order do
	JOIN openmrs.temp_order_mapping om ON do.order_id = om.source_order_id
	WHERE NOT EXISTS (
		SELECT 1 FROM openmrs.drug_order 
		WHERE order_id = om.target_order_id
	);

-- Migrate order_group records
INSERT INTO openmrs.order_group (
    order_set_id,
    patient_id,
    encounter_id,
    creator,
    date_created,
    voided,
    voided_by,
    date_voided,
    void_reason,
    changed_by,
    date_changed,
    uuid,
    order_group_reason,
    parent_order_group,
    previous_order_group
)
SELECT 
    og.order_set_id,
    ptm.target_patient_id AS patient_id,
    em.target_encounter_id AS encounter_id,
    COALESCE(og.creator, 1) AS creator, -- Default to admin if no mapping
    og.date_created,
    og.voided,
    og.voided_by,
    og.date_voided,
    og.void_reason,
    og.changed_by,
    og.date_changed,
    og.uuid,
    og.order_group_reason,
    NULL AS parent_order_group, -- Will update in step 4
    NULL AS previous_order_group -- Will update in step 4
FROM 
    awendodice.order_group og
JOIN 
   openmrs.temp_patient_id_mapping ptm ON og.patient_id = ptm.source_patient_id
JOIN 
    openmrs.temp_encounter_mapping em ON og.encounter_id = em.source_encounter_id
WHERE 
    NOT EXISTS (SELECT 1 FROM openmrs.order_group WHERE uuid = og.uuid);

-- Populate order_group mapping table
INSERT INTO openmrs.temp_order_group_mapping (
    source_order_group_id,
    target_order_group_id,
    source_uuid,
    merge_status
)
SELECT 
    og.order_group_id,
    new_og.order_group_id,
    og.uuid,
    'MIGRATED'
FROM 
    awendodice.order_group og
JOIN 
    openmrs.order_group new_og ON og.uuid = new_og.uuid
WHERE 
    NOT EXISTS (
        SELECT 1 FROM openmrs.temp_order_group_mapping 
        WHERE source_order_group_id = og.order_group_id
    );

-- Update parent/previous order_group references (recursive relationships)
UPDATE openmrs.order_group dest
JOIN awendodice.order_group src ON dest.uuid = src.uuid
LEFT JOIN openmrs.temp_order_group_mapping pgm ON src.parent_order_group = pgm.source_order_group_id
LEFT JOIN openmrs.temp_order_group_mapping prevgm ON src.previous_order_group = prevgm.source_order_group_id
SET 
    dest.parent_order_group = pgm.target_order_group_id,
    dest.previous_order_group = prevgm.target_order_group_id
WHERE 
    src.parent_order_group IS NOT NULL OR 
    src.previous_order_group IS NOT NULL;

        
    -- Migrate observations
    INSERT IGNORE INTO openmrs.obs (
        person_id, concept_id, encounter_id, order_id,
        obs_datetime, location_id, obs_group_id, accession_number,
        value_group_id, value_coded, value_coded_name_id, value_drug,
        value_datetime, value_numeric, value_modifier, value_text,
        value_complex, comments, creator, date_created,
        voided, voided_by, date_voided, void_reason,
        uuid, previous_version, form_namespace_and_path,
        status, interpretation
    )
    SELECT 
        ptm.target_patient_id AS person_id,
        o.concept_id,
        em.target_encounter_id AS encounter_id,
        om.target_order_id AS order_id,
        o.obs_datetime,
        o.location_id,
        NULL, -- Will update obs_group_id in Step 6
        o.accession_number,
        o.value_group_id,
        o.value_coded,
        o.value_coded_name_id,
        o.value_drug,
        o.value_datetime,
        o.value_numeric,
        o.value_modifier,
        o.value_text,
        o.value_complex,
        o.comments,
        COALESCE(o.creator, 1) AS creator,
        o.date_created,
        o.voided,
        o.voided_by,
        o.date_voided,
        o.void_reason,
        o.uuid,
        NULL, -- Will update previous_version in Step 6
        o.form_namespace_and_path,
        o.status,
        o.interpretation
    FROM 
        awendodice.obs o
    JOIN 
        awendodice.patient p ON o.person_id = p.patient_id
    JOIN 
        temp_patient_id_mapping ptm ON p.patient_id = ptm.source_patient_id
    LEFT JOIN 
        awendodice.encounter e ON o.encounter_id = e.encounter_id
    LEFT JOIN 
        temp_encounter_mapping em ON e.encounter_id = em.source_encounter_id
    LEFT JOIN 
        awendodice.orders ord ON o.order_id = ord.order_id
    LEFT JOIN 
        temp_order_mapping om ON ord.order_id = om.source_order_id
    WHERE 
        NOT EXISTS (SELECT 1 FROM openmrs.obs WHERE uuid = o.uuid);

    -- Track migrated observations
    INSERT INTO temp_obs_mapping (source_obs_id, target_obs_id, source_uuid, merge_status)
    SELECT 
        o.obs_id,
        new_o.obs_id,
        o.uuid,
        'migrated_by_uuid'
    FROM 
        awendodice.obs o
    JOIN 
        openmrs.obs new_o ON o.uuid = new_o.uuid
    WHERE 
        NOT EXISTS (SELECT 1 FROM temp_obs_mapping WHERE source_obs_id = o.obs_id);

    -- Update order relationships (previous_order_id)
    UPDATE openmrs.orders target
    JOIN temp_order_mapping tm ON target.order_id = tm.target_order_id
    JOIN awendodice.orders source ON tm.source_order_id = source.order_id
    LEFT JOIN temp_order_mapping prev_tm ON source.previous_order_id = prev_tm.source_order_id
    SET target.previous_order_id = prev_tm.target_order_id
    WHERE source.previous_order_id IS NOT NULL;

    -- Update order_group_id references if you have order_group tables
    -- This would require a similar approach if you're migrating order groups

    --Update observation relationships (obs_group_id, previous_version)
    UPDATE openmrs.obs target
    JOIN temp_obs_mapping tm ON target.obs_id = tm.target_obs_id
    JOIN awendodice.obs source ON tm.source_obs_id = source.obs_id
    LEFT JOIN temp_obs_mapping group_tm ON source.obs_group_id = group_tm.source_obs_id
    SET target.obs_group_id = group_tm.target_obs_id
    WHERE source.obs_group_id IS NOT NULL;

    UPDATE openmrs.obs target
    JOIN temp_obs_mapping tm ON target.obs_id = tm.target_obs_id
    JOIN awendodice.obs source ON tm.source_obs_id = source.obs_id
    LEFT JOIN temp_obs_mapping prev_tm ON source.previous_version = prev_tm.source_obs_id
    SET target.previous_version = prev_tm.target_obs_id
    WHERE source.previous_version IS NOT NULL;

    -- Verification
    SELECT 
        (SELECT COUNT(*) FROM awendodice.orders) AS source_orders,
        (SELECT COUNT(*) FROM temp_order_mapping) AS migrated_orders,
        (SELECT COUNT(*) FROM awendodice.orders o
         WHERE NOT EXISTS (
             SELECT 1 FROM temp_order_mapping 
             WHERE source_order_id = o.order_id
         )) AS unmigrated_orders,
        (SELECT COUNT(*) FROM awendodice.obs) AS source_obs,
        (SELECT COUNT(*) FROM temp_obs_mapping) AS migrated_obs,
        (SELECT COUNT(*) FROM awendodice.obs o
         WHERE NOT EXISTS (
             SELECT 1 FROM temp_obs_mapping 
             WHERE source_obs_id = o.obs_id
         )) AS unmigrated_obs;

    COMMIT;
    SET FOREIGN_KEY_CHECKS = 1;
    SET autocommit = 1;
    SELECT 'Orders and observations migration completed successfully' AS result;
    
    -- HIV INDEX TESTING RECORDS MIGRATION
	SELECT 'INDEX TESTING & Relationship migration completed successfully' AS result;
    
    SET autocommit = 0;
	SET FOREIGN_KEY_CHECKS = 0;
	START TRANSACTION;
    USE openmrs;
    
    -- Create mapping tables for relationships and patient contacts
    DROP TABLE IF EXISTS  openmrs.temp_relationship_mapping;
    CREATE TABLE IF NOT EXISTS temp_relationship_mapping (
        source_relationship_id INT PRIMARY KEY,
        target_relationship_id INT,
        source_uuid CHAR(38),
        merge_status VARCHAR(20)
    ) ENGINE=InnoDB;
    
    DROP TABLE IF EXISTS  openmrs.temp_patient_contact_mapping;
    CREATE TABLE IF NOT EXISTS temp_patient_contact_mapping (
        source_contact_id INT PRIMARY KEY,
        target_contact_id INT,
        source_uuid CHAR(38),
        merge_status VARCHAR(20)
    ) ENGINE=InnoDB;

    -- Migrate Relationship records  
    Migrate relationships
    INSERT IGNORE INTO openmrs.relationship (
        person_a, relationship, person_b, start_date,
        end_date, creator, date_created, date_changed,
        changed_by, voided, voided_by, date_voided,
        void_reason, uuid
    )
    SELECT 
        ptm_a.target_patient_id AS person_a,
        r.relationship,
        ptm_b.target_patient_id AS person_b,
        r.start_date,
        r.end_date,
        COALESCE(r.creator, 1) AS creator,
        r.date_created,
        r.date_changed,
        r.changed_by,
        r.voided,
        r.voided_by,
        r.date_voided,
        r.void_reason,
        r.uuid
    FROM 
        awendodice.relationship r
    JOIN 
        awendodice.patient pa ON r.person_a = pa.patient_id
    JOIN 
        temp_patient_id_mapping ptm_a ON pa.patient_id = ptm_a.source_patient_id
    JOIN 
        awendodice.patient pb ON r.person_b = pb.patient_id
    JOIN 
        temp_patient_id_mapping ptm_b ON pb.patient_id = ptm_b.source_patient_id
    WHERE 
        NOT EXISTS (SELECT 1 FROM openmrs.relationship WHERE uuid = r.uuid);

    -- Track migrated relationships
    INSERT INTO temp_relationship_mapping (source_relationship_id, target_relationship_id, source_uuid, merge_status)
    SELECT 
        r.relationship_id,
        new_r.relationship_id,
        r.uuid,
        'migrated_by_uuid'
    FROM 
        awendodice.relationship r
    JOIN 
        openmrs.relationship new_r ON r.uuid = new_r.uuid
    WHERE 
        NOT EXISTS (SELECT 1 FROM temp_relationship_mapping WHERE source_relationship_id = r.relationship_id);

    -- Migrate patient contacts
    INSERT IGNORE INTO openmrs.kenyaemr_hiv_testing_patient_contact (
        uuid, obs_group_id, first_name, middle_name,
        last_name, sex, birth_date, physical_address,
        phone_contact, patient_related_to, relationship_type,
        appointment_date, baseline_hiv_status, ipv_outcome,
        patient_id, date_created, changed_by, date_changed,
        voided, voided_by, date_voided, voided_reason,
        marital_status, living_with_patient, pns_approach,
        contact_listing_decline_reason, consented_contact_listing,
        reported_test_date
    )
    SELECT 
        pc.uuid,
        om.target_obs_id AS obs_group_id,
        pc.first_name,
        pc.middle_name,
        pc.last_name,
        pc.sex,
        pc.birth_date,
        pc.physical_address,
        pc.phone_contact,
        ptm.target_patient_id AS patient_related_to,
        pc.relationship_type,
        pc.appointment_date,
        pc.baseline_hiv_status,
        pc.ipv_outcome,
        ptm_patient.target_patient_id AS patient_id,
        pc.date_created,
        pc.changed_by,
        pc.date_changed,
        pc.voided,
        pc.voided_by,
        pc.date_voided,
        pc.voided_reason,
        pc.marital_status,
        pc.living_with_patient,
        pc.pns_approach,
        pc.contact_listing_decline_reason,
        pc.consented_contact_listing,
        pc.reported_test_date
    FROM 
        awendodice.kenyaemr_hiv_testing_patient_contact pc
    JOIN 
        awendodice.patient p ON pc.patient_related_to = p.patient_id
    JOIN 
        temp_patient_id_mapping ptm ON p.patient_id = ptm.source_patient_id
    LEFT JOIN 
        awendodice.patient pat ON pc.patient_id = pat.patient_id
    LEFT JOIN 
        temp_patient_id_mapping ptm_patient ON pat.patient_id = ptm_patient.source_patient_id
    LEFT JOIN 
        awendodice.obs o ON pc.obs_group_id = o.obs_id
    LEFT JOIN 
        temp_obs_mapping om ON o.obs_id = om.source_obs_id
    WHERE 
        NOT EXISTS (SELECT 1 FROM openmrs.kenyaemr_hiv_testing_patient_contact WHERE uuid = pc.uuid);

    -- Track migrated patient contacts
    INSERT INTO temp_patient_contact_mapping (source_contact_id, target_contact_id, source_uuid, merge_status)
    SELECT 
        pc.id,
        new_pc.id,
        pc.uuid,
        'migrated_by_uuid'
    FROM 
        awendodice.kenyaemr_hiv_testing_patient_contact pc
    JOIN 
        openmrs.kenyaemr_hiv_testing_patient_contact new_pc ON pc.uuid = new_pc.uuid
    WHERE 
        NOT EXISTS (SELECT 1 FROM temp_patient_contact_mapping WHERE source_contact_id = pc.id);

    -- Migrate client traces
    INSERT IGNORE INTO openmrs.kenyaemr_hiv_testing_client_trace (
        client_id, uuid, contact_type, status,
        unique_patient_no, facility_linked_to,
        health_worker_handed_to, remarks, date_created,
        changed_by, date_changed, voided, voided_by,
        date_voided, voided_reason, encounter_date,
        appointment_date, reason_uncontacted
    )
    SELECT 
        pcm.target_contact_id AS client_id,
        ct.uuid,
        ct.contact_type,
        ct.status,
        ct.unique_patient_no,
        ct.facility_linked_to,
        ct.health_worker_handed_to,
        ct.remarks,
        ct.date_created,
        ct.changed_by,
        ct.date_changed,
        ct.voided,
        ct.voided_by,
        ct.date_voided,
        ct.voided_reason,
        ct.encounter_date,
        ct.appointment_date,
        ct.reason_uncontacted
    FROM 
        awendodice.kenyaemr_hiv_testing_client_trace ct
    JOIN 
        awendodice.kenyaemr_hiv_testing_patient_contact pc ON ct.client_id = pc.id
    JOIN 
        temp_patient_contact_mapping pcm ON pc.id = pcm.source_contact_id

    WHERE 
        NOT EXISTS (SELECT 1 FROM openmrs.kenyaemr_hiv_testing_client_trace WHERE uuid = ct.uuid);

    -- Migrate patient risk scores
    INSERT IGNORE INTO openmrs.kenyaemr_ml_patient_risk_score (
        source_system_uuid, patient_id, risk_score,
        evaluation_date, creator, date_created,
        changed_by, date_changed, voided, voided_by,
        date_voided, voided_reason, description,
        risk_factors, payload, mflCode, cccNumber,
        Age, average_tca_last5, averagelateness,
        averagelateness_last10, averagelateness_last3,
        averagelateness_last5, Breastfeedingno,
        BreastfeedingNR, Breastfeedingyes, DayFri,
        DayMon, DaySat, DaySun, DayThu, DayTue,
        DayWed, DifferentiatedCarecommunityartdistributionhcwled,
        DifferentiatedCarecommunityartdistributionpeerled,
        DifferentiatedCareexpress, DifferentiatedCarefacilityartdistributiongroup,
        DifferentiatedCarefasttrack, DifferentiatedCarestandardcare,
        GenderFemale, GenderMale, late, late_last10,
        late_last3, late_last5, late_rate, late28,
        late28_rate, MaritalStatusDivorced, MaritalStatusMarried,
        MaritalStatusMinor, MaritalStatusOther, MaritalStatusPolygamous,
        MaritalStatusSingle, MaritalStatusWidow, MonthApr,
        MonthAug, MonthDec, MonthFeb, MonthJan, MonthJul,
        MonthJun, MonthMar, MonthMay, MonthNov, MonthOct,
        MonthSep, most_recent_art_adherencefair,
        most_recent_art_adherencegood, most_recent_art_adherencepoor,
        n_appts, NextAppointmentDate, num_hiv_regimens,
        OptimizedHIVRegimenNo, OptimizedHIVRegimenYes,
        Pregnantno, PregnantNR, Pregnantyes,
        StabilityAssessmentStable, StabilityAssessmentUnstable,
        timeOnArt, unscheduled_rate, visit_1,
        last_dwapi_etl_update
    )
    SELECT 
        prs.source_system_uuid,
        ptm.target_patient_id AS patient_id,
        prs.risk_score,
        prs.evaluation_date,
        COALESCE(prs.creator, 1) AS creator,
        prs.date_created,
        prs.changed_by,
        prs.date_changed,
        prs.voided,
        prs.voided_by,
        prs.date_voided,
        prs.voided_reason,
        prs.description,
        prs.risk_factors,
        prs.payload,
        prs.mflCode,
        prs.cccNumber,
        prs.Age,
        prs.average_tca_last5,
        prs.averagelateness,
        prs.averagelateness_last10,
        prs.averagelateness_last3,
        prs.averagelateness_last5,
        prs.Breastfeedingno,
        prs.BreastfeedingNR,
        prs.Breastfeedingyes,
        prs.DayFri,
        prs.DayMon,
        prs.DaySat,
        prs.DaySun,
        prs.DayThu,
        prs.DayTue,
        prs.DayWed,
        prs.DifferentiatedCarecommunityartdistributionhcwled,
        prs.DifferentiatedCarecommunityartdistributionpeerled,
        prs.DifferentiatedCareexpress,
        prs.DifferentiatedCarefacilityartdistributiongroup,
        prs.DifferentiatedCarefasttrack,
        prs.DifferentiatedCarestandardcare,
        prs.GenderFemale,
        prs.GenderMale,
        prs.late,
        prs.late_last10,
        prs.late_last3,
        prs.late_last5,
        prs.late_rate,
        prs.late28,
        prs.late28_rate,
        prs.MaritalStatusDivorced,
        prs.MaritalStatusMarried,
        prs.MaritalStatusMinor,
        prs.MaritalStatusOther,
        prs.MaritalStatusPolygamous,
        prs.MaritalStatusSingle,
        prs.MaritalStatusWidow,
        prs.MonthApr,
        prs.MonthAug,
        prs.MonthDec,
        prs.MonthFeb,
        prs.MonthJan,
        prs.MonthJul,
        prs.MonthJun,
        prs.MonthMar,
        prs.MonthMay,
        prs.MonthNov,
        prs.MonthOct,
        prs.MonthSep,
        prs.most_recent_art_adherencefair,
        prs.most_recent_art_adherencegood,
        prs.most_recent_art_adherencepoor,
        prs.n_appts,
        prs.NextAppointmentDate,
        prs.num_hiv_regimens,
        prs.OptimizedHIVRegimenNo,
        prs.OptimizedHIVRegimenYes,
        prs.Pregnantno,
        prs.PregnantNR,
        prs.Pregnantyes,
        prs.StabilityAssessmentStable,
        prs.StabilityAssessmentUnstable,
        prs.timeOnArt,
        prs.unscheduled_rate,
        prs.visit_1,
        prs.last_dwapi_etl_update
    FROM 
        awendodice.kenyaemr_ml_patient_risk_score prs
    JOIN 
        awendodice.patient p ON prs.patient_id = p.patient_id
    JOIN 
        temp_patient_id_mapping ptm ON p.patient_id = ptm.source_patient_id

    WHERE 
        NOT EXISTS (
            SELECT 1 FROM openmrs.kenyaemr_ml_patient_risk_score 
            WHERE source_system_uuid = prs.source_system_uuid
            AND patient_id = ptm.target_patient_id
        );

    -- Migrate lab manifests
    INSERT IGNORE INTO openmrs.kenyaemr_order_entry_lab_manifest (
        start_date, end_date, dispatch_date,
        courier, courier_officer, status,
        facility_county, facility_sub_county,
        facility_email, facility_phone_contact,
        clinician_name, clinician_phone_contact,
        lab_poc_phone_number, creator, date_created,
        changed_by, date_changed, voided, voided_by,
        date_voided, voided_reason, uuid, identifier,
        manifest_type
    )
    SELECT 
        lm.start_date,
        lm.end_date,
        lm.dispatch_date,
        lm.courier,
        lm.courier_officer,
        lm.status,
        lm.facility_county,
        lm.facility_sub_county,
        lm.facility_email,
        lm.facility_phone_contact,
        lm.clinician_name,
        lm.clinician_phone_contact,
        lm.lab_poc_phone_number,
        COALESCE(lm.creator, 1) AS creator,
        lm.date_created,
        lm.changed_by,
        lm.date_changed,
        lm.voided,
        lm.voided_by,
        lm.date_voided,
        lm.voided_reason,
        lm.uuid,
        lm.identifier,
        lm.manifest_type
    FROM 
        awendodice.kenyaemr_order_entry_lab_manifest lm
    WHERE 
        NOT EXISTS (SELECT 1 FROM openmrs.kenyaemr_order_entry_lab_manifest WHERE uuid = lm.uuid);

    -- Migrate lab manifest orders
    INSERT IGNORE INTO openmrs.kenyaemr_order_entry_lab_manifest_order (
        manifest_id, order_id, sample_type, payload,
        date_sent, status, result, result_date,
        sample_collection_date, sample_separation_date,
        date_created, voided, date_voided, voided_reason,
        uuid, last_status_check_date, date_sample_received,
        date_sample_tested, order_type, batch_number,
        results_pulled_date, results_dispatch_date
    )
    SELECT 
        new_lm.id AS manifest_id,
        om.target_order_id AS order_id,
        lmo.sample_type,
        lmo.payload,
        lmo.date_sent,
        lmo.status,
        lmo.result,
        lmo.result_date,
        lmo.sample_collection_date,
        lmo.sample_separation_date,
        lmo.date_created,
        lmo.voided,
        lmo.date_voided,
        lmo.voided_reason,
        lmo.uuid,
        lmo.last_status_check_date,
        lmo.date_sample_received,
        lmo.date_sample_tested,
        lmo.order_type,
        lmo.batch_number,
        lmo.results_pulled_date,
        lmo.results_dispatch_date
    FROM 
        awendodice.kenyaemr_order_entry_lab_manifest_order lmo
    JOIN 
        awendodice.kenyaemr_order_entry_lab_manifest lm ON lmo.manifest_id = lm.id
    JOIN 
        openmrs.kenyaemr_order_entry_lab_manifest new_lm ON lm.uuid = new_lm.uuid
    JOIN 
        awendodice.orders o ON lmo.order_id = o.order_id
    LEFT JOIN 
        temp_order_mapping om ON o.order_id = om.source_order_id
    WHERE 
        NOT EXISTS (SELECT 1 FROM openmrs.kenyaemr_order_entry_lab_manifest_order WHERE uuid = lmo.uuid);

    -- Verification
    SELECT 
        (SELECT COUNT(*) FROM awendodice.relationship) AS source_relationships,
        (SELECT COUNT(*) FROM temp_relationship_mapping) AS migrated_relationships,
        (SELECT COUNT(*) FROM awendodice.kenyaemr_hiv_testing_patient_contact) AS source_contacts,
        (SELECT COUNT(*) FROM temp_patient_contact_mapping) AS migrated_contacts,
        (SELECT COUNT(*) FROM awendodice.kenyaemr_hiv_testing_client_trace) AS source_traces,
        (SELECT COUNT(*) FROM openmrs.kenyaemr_hiv_testing_client_trace ct
         JOIN temp_patient_contact_mapping pcm ON ct.client_id = pcm.target_contact_id) AS migrated_traces,
        (SELECT COUNT(*) FROM awendodice.kenyaemr_ml_patient_risk_score) AS source_risk_scores,
        (SELECT COUNT(*) FROM openmrs.kenyaemr_ml_patient_risk_score prs
         JOIN temp_patient_id_mapping ptm ON prs.patient_id = ptm.target_patient_id) AS migrated_risk_scores,
        (SELECT COUNT(*) FROM awendodice.kenyaemr_order_entry_lab_manifest) AS source_manifests,
        (SELECT COUNT(*) FROM openmrs.kenyaemr_order_entry_lab_manifest) AS migrated_manifests,
        (SELECT COUNT(*) FROM awendodice.kenyaemr_order_entry_lab_manifest_order) AS source_manifest_orders,
        (SELECT COUNT(*) FROM openmrs.kenyaemr_order_entry_lab_manifest_order) AS migrated_manifest_orders;

    COMMIT;
    SET FOREIGN_KEY_CHECKS = 1;
    SET autocommit = 1;
    
    SELECT 'KenyaEMR related tables data migration completed successfully' AS result;
    
    -- UPDATE THE LOCATION ID IN  ALL THE MIGRATED TABLES

	SET autocommit = 0;
	SET FOREIGN_KEY_CHECKS = 0;
	START TRANSACTION;
	use openmrs;
    -- Identify the target location ID you want to use
    -- Replace 'Desired Location Name' with your specific location name
    SET @target_location_id = (SELECT location_id FROM location WHERE name = 'Kisumu Police Line Dispensary' LIMIT 1);
	
    -- Verify we found the location
    SELECT @target_location_id;
    -- Update encounter table
    UPDATE encounter SET location_id = @target_location_id WHERE location_id IS NULL;
    
    -- Update visit table
    UPDATE visit SET location_id = @target_location_id WHERE location_id IS NULL;
    
    -- Update obs table
    UPDATE obs SET location_id = @target_location_id WHERE location_id IS NULL;
    
    -- Update patient_identifier table
    UPDATE patient_identifier SET location_id = @target_location_id WHERE location_id IS NULL;
    

    -- Update patient_program table
    UPDATE patient_program SET location_id = @target_location_id WHERE location_id IS NULL;
    

    -- Verification: Count records updated in each table
    SELECT 
        'encounter' AS table_name, 
        COUNT(*) AS records_updated 
    FROM encounter 
    WHERE location_id = @target_location_id
    UNION ALL
    SELECT 'visit', COUNT(*) FROM visit WHERE location_id = @target_location_id
    UNION ALL
    SELECT 'obs', COUNT(*) FROM obs WHERE location_id = @target_location_id;

    COMMIT;
    SET FOREIGN_KEY_CHECKS = 1;
    SET autocommit = 1;
    SELECT CONCAT('Location ID updated to ', @target_location_id, ' across all tables') AS result;



    


