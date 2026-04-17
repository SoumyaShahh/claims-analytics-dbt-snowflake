{{ config(materialized='view') }}

with source as (
    select * from {{ source('synthea_raw', 'raw_claims') }}
),

renamed as (
    select
        id                                                       as claim_id,
        patientid                                                as patient_id,
        providerid                                               as provider_id,
        primarypatientinsuranceid                                as primary_payer_id,
        secondarypatientinsuranceid                              as secondary_payer_id,
        departmentid                                             as department_id,
        patientdepartmentid                                      as patient_department_id,
        diagnosis1                                               as primary_diagnosis_code,
        diagnosis2                                               as diagnosis_code_2,
        diagnosis3                                               as diagnosis_code_3,
        diagnosis4                                               as diagnosis_code_4,
        diagnosis5                                               as diagnosis_code_5,
        diagnosis6                                               as diagnosis_code_6,
        diagnosis7                                               as diagnosis_code_7,
        diagnosis8                                               as diagnosis_code_8,
        referringproviderid                                      as referring_provider_id,
        appointmentid                                            as encounter_id,
        cast(currentillnessdate as date)                         as illness_start_date,
        cast(servicedate as date)                                as service_date,
        supervisingproviderid                                    as supervising_provider_id,
        status1                                                  as primary_status,
        status2                                                  as secondary_status,
        statusp                                                  as patient_status,
        coalesce(outstanding1, 0)                                as primary_outstanding_amount,
        coalesce(outstanding2, 0)                                as secondary_outstanding_amount,
        coalesce(outstandingp, 0)                                as patient_outstanding_amount,
        coalesce(outstanding1, 0)
          + coalesce(outstanding2, 0)
          + coalesce(outstandingp, 0)                            as total_outstanding_amount,
        cast(lastbilleddate1 as date)                            as primary_last_billed_date,
        cast(lastbilleddate2 as date)                            as secondary_last_billed_date,
        cast(lastbilleddatep as date)                            as patient_last_billed_date,
        healthcareclaimtypeid1                                   as primary_claim_type_id,
        healthcareclaimtypeid2                                   as secondary_claim_type_id
    from source
)

select * from renamed