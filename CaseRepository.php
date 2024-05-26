<?php


namespace App\Repositories\BuildingManagement;


use App\Action;
use App\AttachmentsTypes;
use App\Common\MybosBaseCaseStatus;
use App\Common\MybosBaseCaseType;
use App\Helpers\Helpers as Helpers2;
use App\Common\MybosCaseArea;
use App\Common\MybosCaseMailType;
use App\Common\MybosCasePriority;
use App\Common\MybosCaseSummaryItem;
use App\Common\MybosCategoryType;
use App\Common\MybosMessaging;
use App\Common\MybosDateTimeFormat;
use App\Common\MybosFileType;
use App\Common\MybosImageSize;
use App\Common\MybosUserGroup;
use App\Common\MybosUserType;
use App\DatabaseClient\MongoDbNative;
use App\DataModels\CaseData;
use App\DataModels\Common\Building\EmailTemplateSettingsData;
use App\DataModels\Common\Case\CaseContractorData;
use App\DataModels\Common\Case\CaseContractorLinkData;
use App\DataModels\Common\Case\CaseEmailData;
use App\DataModels\Common\Case\CaseInvoiceData;
use App\DataModels\Common\Case\CaseInventoryUsageData;
use App\DataModels\Common\Case\CaseLogData;
use App\DataModels\Filters\CasesListFilter;
use App\DataModels\S3FileModel;
use App\Helpers\LinkedTemplateProcessor;
use App\Helpers\MybosCase as MybosCaseHelper;
use App\Helpers\MybosFile;
use App\Helpers\MybosString;
use App\Helpers\MybosTime;
use App\Helpers\MybosUserSession;
use App\Mail\EmailAuth;
use App\Mail\StandardTemplatedEmail;
use App\Models\Apartments\Apartment;
use App\Models\Assets\Asset;
use App\Models\Cases\CaseContractorLink;
use App\Models\Cases\CaseInventoryUsage;
use App\Helpers;
use App\Models\Building\Building;
use App\Models\Cases\CaseApartment;
use App\Models\Cases\CaseAsset;
use App\Models\Cases\CaseAttachment;
use App\Models\Cases\CaseContractor;
use App\Models\Cases\CaseContractorContact;
use App\Models\Cases\CaseFolder;
use App\Models\Cases\CaseInventory;
use App\Models\Cases\CaseInvoice;
use App\Models\Cases\CaseEmail;
use App\Models\Cases\CaseQuote;
use App\Models\Cases\CaseQuoteInvoice;
use App\Models\Cases\Cases;
use App\Models\Cases\CaseStatus;
use App\Models\Categories\Category;
use App\Models\Company;
use App\Models\Contractors\Contractor;
use App\Models\Contractors\ContractorContact;
use App\Models\Folder\Folder;
use App\Models\Inventory\Inventory;
use App\Models\User;
use App\Models\UserBrowsers;
use App\MYBOS\Attachment\Type;
use App\Services\Excel;
use App\Services\ImageProcessor;
use App\Services\TemplateProcessor;
use Illuminate\Support\Facades\Storage;
use Io238\ISOCountries\Models\Currency;
use MongoDB\BSON\UTCDateTime;
use PhpOffice\PhpWord\Exception\Exception;
use PhpOffice\PhpWord\Shared\Html;
use PhpOffice\PhpWord\Writer\Word2007\Element\Container;
use App\Storage\AwsS3;
use Carbon\Carbon;
use Carbon\CarbonInterface;
use Illuminate\Support\Facades\Hash;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Mail;
use Illuminate\Support\Facades\Request;
use Illuminate\Support\Facades\Response;
use Illuminate\Support\Facades\Validator;
use Illuminate\Validation\Rules\In;
use MongoDB\BSON\ObjectId;
use PhpOffice\PhpSpreadsheet\Spreadsheet;
use PhpOffice\PhpSpreadsheet\Writer\Xlsx;
use PDF;
use PhpOffice\PhpWord\Settings as WordSettings;
use PhpOffice\PhpWord\Shared\XMLWriter;
use PhpOffice\PhpWord\Writer\Word2007\Element\AbstractElement;
use App\Repositories\ContractorRepository as GlobalContractorRepository;
use function Psy\debug;
use App\Helpers\MQL\MQLCases;


class CaseRepository
{
    // Status
    public const STATUS_NEW = 0;
    public const STATUS_PROGRESS = 1;
    public const STATUS_COMPLETED = 2;
    public const STATUS_DELETED = 3;
    public const STATUS_P_DELETED = 4;
    public const STATUS_DRAFT = 5;
    // Area
    public const AREA_PRIVATE = MybosCaseArea::PRIVATE;
    public const AREA_COMMON = MybosCaseArea::COMMON_ASSET;
    public const AREA_NON_ASSET = MybosCaseArea::COMMON_NON_ASSET;
    // Priority
    public const PRIORITY_LOW = MybosCasePriority::LOW;
    public const PRIORITY_MEDIUM = MybosCasePriority::MEDIUM;
    public const PRIORITY_HIGH = MybosCasePriority::HIGH;
    public const PRIORITY_URGENT = MybosCasePriority::URGENT;
    // Mail
    public const MAIL_SUMMARY = 0;
    public const MAIL_QUOTE_REQUEST = 1;
    public const MAIL_WORK_ORDER = 2;

    // Message
    public const FORM_MANAGER = 0;
    public const FORM_RESIDENT = 1;
    const ASSETS_STATUS_ACTIVE = 1;

    public static function create(array|CaseData $caseData, Building $building, User $user = null): array|Cases
    {
        if ($caseData instanceof CaseData) {
            /** If input data is already of type specific data model type.
             *      ex: NoticeData, UserData, etc. *
             */
            $arrayCaseData = $caseData->toArray();
        } else {
            /**     If input data is only an array type, ensure that it
             *  is properly initialized by instantiating as the specific data model.
             *      ex: NoticeData, UserData, etc.
             */
            $caseDataModel = new CaseData($caseData);
            $arrayCaseData = $caseDataModel->toArray();
        }

        // Initialise data.
        $arrayCaseData['building_id'] = $building['_id'];
        if ($user != null) {
            $arrayCaseData['manager_id'] = $user['_id'];
            $arrayCaseData['logged_by'] = $user['first_name'] . ' ' . $user['last_name'];
        }

        $caseModelObject = new Cases($arrayCaseData);
        if ($caseModelObject->save()) {
            return $caseModelObject;
        }
        return ['error' => 'Cannot create [case].'];
    }

    public static function _old_create(Building $building, User $user)
    {
        /**
         * NOTES:
         *      - No need to get status for "Drafts", a case with null status is considered draft. *
         *      - [status_id] and [number] will just be filled automatically after creating case, see case model's boot() function...
         *      - sub_status(from V3) is now integrated with status_id
         */

        // Set something on CaseData so it does not return a model template but an actual empty caseData.
        $caseData = new CaseData([
            'building_id' => $building['_id'],
            'subject' => '',
            'area' => [MybosCaseArea::COMMON_ASSET],
            'logged_by' => '[' . $user['email'] . '] ' . $user['first_name'] . ' ' . $user['last_name'],
            'manager_id' => $user['_id'],
        ]);

        $case = new Cases($caseData->toArray());
        if ($case->save()) {
            return $case;
        }
        return ['status' => 422, 'message' => 'Cannot create case...'];
    }

    /**
     * @param Cases $case
     * @param array $data
     * @param array $caseCloneBuildings
     * @return Cases|array|string[]
     */
    public static function update(Cases $case, array $data): array|Cases
    {
        $case->subject = $data['subject'] ?? $case->subject;
        $case->detail = $data['detail'] ?? $case->detail;
        $case->type_id = $data['type_id'] ?? $case->type_id;
        $case->priority_id = $data['priority_id'] ?? $case->priority_id;

        $case->start = isset($data['start']) ? MybosTime::CarbonToUTCDateTime(Carbon::createFromFormat('M j, Y', $data['start'], $case->building->timezone)->startOfDay()) : $case->start;
        $case->due = isset($data['due']) ? MybosTime::CarbonToUTCDateTime(Carbon::createFromFormat('M j, Y', $data['due'], $case->building->timezone)->startOfDay()) : $case->due;
        $case->completion_date = isset($data['completion_date']) ? MybosTime::CarbonToUTCDateTime(Carbon::createFromFormat('M j, Y', $data['completion_date'], $case->building->timezone)->startOfDay()) : $case->completion_date;

        /** Fill default case status if not is specified.
         *  At this point building should already be existing, so we get case status id via the parent building...
         **/

        if ($case->status_id != $data['status_id']) {
            $dateNow = MybosTime::dbCompatible(MybosTime::now());
            $status = Category::where('name', MybosBaseCaseStatus::COMPLETED)->first();
            if ($data['status_id'] == $status->_id) {
                $case->completion_date = $dateNow;
            }
        }

        $case->status_id = isset($data['status_id']) && !empty($data['status_id']) && $data['status_id'] != 'null' ? $data['status_id'] : $case->status_id;
        if (!isset($data['is_draft'])) {
            if (empty($case->status_id)) {
                $caseStatusNew = $case->building->getCaseStatusBy('name', MybosBaseCaseStatus::NEW);
                $case->status_id = $caseStatusNew->_id;
            }
        }
        $case->is_report = $data['is_report'] ?? $case->is_report;
        $case->duplicate_case_id = $data['duplicate_case_id'] ?? $case->duplicate_case_id;
        $case->history_note = $data['history_note'] ?? '';
        $case->purchase_order_number = $data['purchase_order_number'] ?? null;

        // Persists case asset info.
        $pCase = CaseRepository::persistAssetInfo($case, $data);
        if (!$pCase instanceof Cases) {
            return ['error' => $pCase['error']];
        }

        if ($pCase->save()) {
            return $pCase;
        }
        return ['error' => 'Case cannot be updated. Please see system logs...'];
    }

    /**
     * @param Cases $sourceCase
     * @param Building $destinationBuilding
     * @return Cases|string[]
     */
    public static function cloneCaseToBuilding(Cases $sourceCase, Building $destinationBuilding): array|Cases
    {
        $cloneCase = self::create(new CaseData([
            'duplicate_case_id' => $sourceCase['_id'],

            // No need to copy case [number] as it should have its own number in the target building.
            //'number' => $sourceCase['number'],

            // Area needs to be set to 'common non-asset' to avoid linking asset and contractors that don't exist in the target building..
            'area' => [MybosCaseArea::COMMON_NON_ASSET],

            'subject' => $sourceCase['subject'],
            'detail' => $sourceCase['detail'],
            'history_note' => $sourceCase['history_note'],
            'logged_by' => $sourceCase['logged_by'],
            'is_report' => $sourceCase['is_report'],

            'start' => $sourceCase['start'],
            'due' => $sourceCase['due'],
            'completion_date' => $sourceCase['completion_date'],
        ]), $destinationBuilding, null);

        if (!$cloneCase instanceof Cases) {
            return ['error' => 'Cannot create duplicated case to target building.'];
        }

        // Copy logs manually as mongodb _id needs to be created as well as datetimes.
        $cloneCaseLogs = [];
        foreach ($sourceCase['logs'] as $log){
            $caseLogData = new CaseLogData([
                'detail' => $log['detail'],
                'created_at' => MybosTime::dbCompatible($log['created_at']),
                'updated_at' => MybosTime::dbCompatible($log['updated_at']),
            ]);
            $cloneCaseLogs[] = $caseLogData;
        }
        $cloneCase['logs'] = $cloneCaseLogs;

        // Set default case type to "Repair & Maintenance".
        $caseTypeMaintenanceRequest = Category::where('building_id', $destinationBuilding['_id'])
            ->where('type', MybosCategoryType::CASE_TYPE)
            ->where('name', MybosBaseCaseType::REPAIR_MAINTENANCE)->first();
        if ($caseTypeMaintenanceRequest instanceof Category) {
            $cloneCase->type()->associate($caseTypeMaintenanceRequest);
        }

        // Set default case status to "New".
        $caseStatusNew = Category::where('building_id', $destinationBuilding['_id'])
            ->where('type', MybosCategoryType::CASE_STATUS)
            ->where('name', MybosBaseCaseStatus::NEW)->first();
        if ($caseStatusNew instanceof Category) {
            $cloneCase->status()->associate($caseStatusNew);
        }

        // Set default case priority to "Low".
        $caseStatusNew = Category::where('building_id', $destinationBuilding['_id'])
            ->where('type', MybosCategoryType::CASE_STATUS)
            ->where('name', MybosBaseCaseStatus::NEW)->first();
        if ($caseStatusNew instanceof Category) {
            $cloneCase->status()->associate($caseStatusNew);
        }

        if (!$cloneCase->save()) {
            return ['error' => 'Case was cloned but failed to setup case default settings.'];
        }

        // Clone photos
        self::cloneCasePhotosOrDocuments($sourceCase, $cloneCase);

        // Clone documents
        self::cloneCasePhotosOrDocuments($sourceCase, $cloneCase, 'document');

        return $cloneCase;
    }

    /**
     * @param Cases $sourceCase
     * @param Cases $cloneCase
     * @param string $strFileType
     * @return void
     */
    public static function cloneCasePhotosOrDocuments(Cases $sourceCase, Cases $cloneCase, string $strFileType = 'photo'): void
    {
        $fieldName = 'photos';
        $mybosFileType = MybosFileType::CASE_PHOTO;
        if($strFileType == 'document'){
            $fieldName = 'documents';
            $mybosFileType = MybosFileType::CASE_DOCUMENT;
        }

        if (isset($sourceCase[$fieldName])) {
            $s3Bucket = new AwsS3();
            foreach ($sourceCase[$fieldName] as $photoDoc) {
                $photoDocFile = $photoDoc['file'] ?? [];
                if (!empty($photoDocFile['s3key'])) {
                    $secure_url = $s3Bucket->preSecureDocumentUrl($photoDocFile['s3key']);

                    // Save file temporarily at '/tmp' folder.
                    $file_path = '/tmp/' . $photoDocFile['file_name'];

                    // Skip failed download of photo/doc duplicate and log incident.
                    if (!MybosFile::saveFileFromUrl($secure_url, $file_path)) {
                        Log::warning("Case {$strFileType} duplicate - download failed, file not added.", [
                            'source_case' => $sourceCase['_id'],
                            'target_building' => $cloneCase['building_id']
                        ]);
                        continue;
                    }

                    // Skip failed upload of photo/doc duplicate and log incident.
                    $s3UploadFileResp = AwsS3::uploadAsFileType($file_path, $mybosFileType, $cloneCase);
                    if(!$s3UploadFileResp instanceof S3FileModel){
                        Log::warning("Case {$strFileType} duplicate - upload failed, file not added.", [
                            'source_case' => $sourceCase['_id'],
                            'target_building' => $cloneCase['building_id']
                        ]);
                        continue;
                    }

                    $photoDocData = [
                        '_id' => new ObjectId(),
                        'owner_id' => $cloneCase['building_id'],
                        'type' => MybosUserType::BUILDING,
                        'file' => $s3UploadFileResp
                    ];
                    if ($strFileType == 'document') {
                        $addPhotoDocResult = $cloneCase->addDocument($photoDocData);
                    } else {
                        $addPhotoDocResult = $cloneCase->addPhoto($photoDocData);
                    }
                    if (!$addPhotoDocResult) {
                        Log::warning("Case {$strFileType} duplicate - save failed, file not added.", [
                            'source_case' => $sourceCase['_id'],
                            'target_building' => $cloneCase['building_id']
                        ]);
                        continue;
                    }

                    Log::info("Case {$strFileType} duplicate successfully added.", [
                        'source_case' => $sourceCase['_id'],
                        'target_building' => $cloneCase['building_id']
                    ]);
                }
            }
        }
    }

    public static function addContractor(Cases $case, array $data)
    {
        $fContractor = Contractor::find($data['contractor_id']);
        if ($fContractor instanceof Contractor) {
            $contactIds = [];
            foreach ($fContractor->contacts as $eachContacts) {
                if ($eachContacts['primary'] == 1) {
                    $contactIds[] = new ObjectId((string)$eachContacts['uid']);
                }
            }

            $rv = $case->contractors()->save(
                new CaseContractor([
                    'contractor_id' => $data['contractor_id'],
                    'contacts' => $contactIds
                ])
            );
            if ($rv) {
                return $case;
            }

            return ['error' => 'Failed to add contractor'];
        } else {
            return ['error' => 'Case cannot be updated. One of the added contractors cannot be verified...'];
        }
    }


    /**
     * @param Cases $case
     * @param $assetData
     * @return Cases|string[]
     */
    public static function persistAssetInfo(Cases $case, $assetData): array|Cases
    {
        /** NOTE: This function does not save the input case, it will only persist asset info data.
         * Saving will still be needed after calling this function. **/
        $caseArea = $assetData['area'] ?? $case->area;
        if (count(array_intersect($caseArea, MybosCaseArea::getArray())) == count($assetData['area'])) {
            $case->area = $caseArea;
        }

        /** Save all [contractors] and [contacts] **/
        $selectedContractorIds = array_unique($assetData['contractors'] ?? []);
        $selectedContractorContactIds = array_unique($assetData['contacts'] ?? []);
        if (!empty($selectedContractorIds)) {
            $caseContractors = [];
            foreach ($selectedContractorIds as $contractorId) {
                $fContractor = Contractor::find($contractorId);
                $caseContractor = [];
                if ($fContractor instanceof Contractor) {
                    $caseContractor['contractor_id'] = $contractorId;
                    $caseContractor['contacts'] = [];

                    $fContractorContacts = array_map(function ($eachContacts) use ($contractorId) {
                        return (string)$eachContacts['uid'];
                    }, $fContractor->contacts);

                    foreach ($selectedContractorContactIds as $contactId) {
                        if (in_array($contactId, $fContractorContacts) !== false) {
                            $caseContractor['contacts'][] = new ObjectId($contactId);
                        }
                    }
                    $caseContractors[] = new CaseContractorData($caseContractor);
                } else {
                    return ['error' => 'Case cannot be updated. One of the added contractors cannot be verified...'];
                }
            }
            $case->contractors = $caseContractors;
        }

        /** Save all [assets] **/
        $selectedAssetIds = array_unique($assetData['assets'] ?? []);
        if (in_array(MybosCaseArea::COMMON_ASSET, $caseArea) && !empty($selectedAssetIds)) {
            $caseAssets = [];
            foreach ($selectedAssetIds as $assetId) {
                // Just in case passed on asset IDs are still in ObjectId formats.
                if (!empty($assetId['$oid'])) {
                    $assetId = $assetId['$oid'];
                }

                $fAsset = Asset::find($assetId);
                if ($fAsset instanceof Asset) {
                    $assetObjectId = new ObjectId($assetId);
                    $caseAssets[] = $assetObjectId;
                } else {
                    return ['error' => 'Case cannot be updated. One of the added assets cannot be verified...'];
                }

            }
            $case->assets = $caseAssets;
            if (!in_array(MybosCaseArea::PRIVATE, $caseArea)) {
                $case->apartments = [];
            }
        }

        /** Save all [apartments] **/
        $selectedApartmentIds = array_unique($assetData['apartments'] ?? []);
        if (in_array(MybosCaseArea::PRIVATE, $caseArea) && !empty($selectedApartmentIds)) {
            $caseApartments = [];
            foreach ($selectedApartmentIds as $apartmentId) {
                $fApartment = Apartment::find($apartmentId);
                if ($fApartment instanceof Apartment) {
                    $apartmentObjectId = new ObjectId($apartmentId);
                    $caseApartments[] = $apartmentObjectId;
                } else {
                    return ['error' => 'Case cannot be updated. One of the added apartment cannot be verified...'];
                }
            }
            $case->apartments = $caseApartments;
            if (!in_array(MybosCaseArea::COMMON_ASSET, $caseArea)) {
                $case->assets = [];
            }
        }

        if (in_array(MybosCaseArea::COMMON_NON_ASSET, $caseArea)) {
            $case->apartments = [];
            $case->assets = [];
        }

        return $case;
    }

    /**
     * @param Cases $case
     * @param string $caseFileType , options: 'photos' / 'documents'
     * @param string $mode
     * @return array
     */
    public static function getAllDocsPhotos(Cases $case, string $caseFileType = 'photos', string $mode = 'normal'): array
    {
        // Get case again to refresh case copy.
        $case = Cases::find($case->_id);

        $mybosDateTimeFormat = MybosDateTimeFormat::STANDARD;
        if ($caseFileType == 'documents') {
            $mybosDateTimeFormat = MybosDateTimeFormat::AUSTRALIA_DATE;
        }

        $allCaseFiles = [];
        switch ($mode) {
            case 'with_secure_url':
                $s3Bucket = new AwsS3();
                foreach ($case[$caseFileType] as $caseFile) {
                    $caseS3File = new S3FileModel($caseFile['file']);
                    $s3Bucket->appendSecureUrl($caseS3File);
                    $arrayCaseS3File = (array)$caseS3File;
                    $arrayCaseS3File['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($arrayCaseS3File['created_at']);
                    $arrayCaseS3File['created_at'] = MybosTime::format_mongoDbTimeUsingBuildingTimezone($arrayCaseS3File['created_at'], $case->building->timezone, $mybosDateTimeFormat);
                    $caseFile['file'] = $arrayCaseS3File;
                    $caseFile['file']['web_secure_url'] = '/document/'.base64_encode($caseFile['file']['s3key']);
                    $allCaseFiles[] = $caseFile;
                }
                break;
            default:
            case 'normal':
                foreach ($case[$caseFileType] as $caseFile) {
                    $caseFile['file'] = [
                        'created_at' => $caseFile['file']['created_at'] ? MybosTime::format_mongoDbTimeUsingBuildingTimezone($caseFile['file']['created_at'], $case->building->timezone, $mybosDateTimeFormat) : '',
                    ];
                    $allCaseFiles[] = $caseFile;
                }
                break;
        }

        return $allCaseFiles;
    }

    /**
     * @param string $caseId
     * @param Building $building
     * @return Cases|array
     */
    public static function getCase(string $caseId, Building $building): array|bool
    {
        $case = Cases::find($caseId);
        if (!$case instanceof Cases) {
            return false;
        }

        $case['history_note_format'] = str_replace("<br />", "\n", $case['history_note']);
        $case['created_at_formatted'] = MybosTime::format_mongoDbTimeUsingBuildingTimezone(MybosTime::CarbonToUTCDateTime($case['created_at']), $building['timezone'],  MybosDateTimeFormat::AUSTRALIA_DATE);
        $case['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($case['created_at']);
        $case['start_8601'] = MybosTime::UTCDateTimeToCarbon($case['start']);
        $case['due_8601'] = MybosTime::UTCDateTimeToCarbon($case['due']);
        $caseFullDetails = self::getCaseFullDetails($caseId);
        /** format sent date... **/
        foreach ($caseFullDetails['emails'] as &$email) {
            $email['sent_at_8601'] = MybosTime::UTCDateTimeToCarbon($email['created_at']);
            $email['sent_at'] = MybosTime::format_mongoDbTimeUsingBuildingTimezone($email['created_at'], $building['timezone'], 'd/m/Y h:iA');
        }

        /** Convert dates to actual building's timezone... **/
        self::convertDatesToBuildingTimezone($caseFullDetails, $building);

        /** Get pre-secured url for all images/files... **/
        self::getSecureFileUrls($caseFullDetails);

        /** Get case's detailed contractor data... **/
        $caseFullDetails['contractors'] = self::getDetailedContractorsWithDocuments((array)$caseFullDetails['contractors'], $building);

        /** Set/Add contractor 'status' from $caseFullDetails['contractors'] 'status' field... **/
        $case['contractors'] = collect($case['contractors'])->map(function ($baseContractor) use ($caseFullDetails){
            $matchedContractor = collect($caseFullDetails['contractors'])->first(function ($fullContractor) use ($baseContractor) {
                return $baseContractor['contractor_id'] == (string)$fullContractor['_id'];
            });
            if ($matchedContractor != null) {
                $baseContractor['status'] = $matchedContractor['status'];
            }
            return $baseContractor;
        });

        /** Get case's next 100 email reference numbers for use in sending work-order, quote or summary emails. **/
        $caseFullDetails['emails_next_reference_numbers'] = $case->generateNextEmailReferenceNumbers(100);

        /** Get case's resized photos. **/
        $resizedPhotoArray = $case->generateUpdateResizedPhotos();

        $caseFullDetails['resized_photos'] = $resizedPhotoArray;
        return [
            'case' => $case,
            'case_full_detail' => $caseFullDetails,
            'building_contractors' => $case->building->contractors,
        ];
    }

    /**
     * @param string $case_id
     * @return array|false|object
     */
    public static function checkCaseIsMaintenanceRequest(string $case_id): object|false|array
    {
        $mongoDb = app('MongoDbClient');
        $collection = $mongoDb->getCollection('maintenance_requests');
        $result = $collection->findOne(
            ['case_id' => $case_id],
            [
                'projection' => [
                    '_id' => 1,
                    'comments' => 1,
                ]
            ]
        );
        if (!empty($result)) {
            return $result;
        }
        return false;
    }

    private static function convertDatesToBuildingTimezone(&$fullCaseDetailsData, Building $building)
    {
        $localeProfile = $building->locale_profile->toArray();
        $fullCaseDetailsData['created_at_8601'] = $fullCaseDetailsData['created_at'] ? MybosTime::UTCDateTimeToCarbon($fullCaseDetailsData['created_at']) : '';
        $fullCaseDetailsData['created_at_locale'] = $fullCaseDetailsData['created_at'] ? MybosTime::format_mongoDbTimeUsingBuildingTimezone($fullCaseDetailsData['created_at'], $localeProfile['timezone'], $localeProfile['date_format']) : '';
        $fullCaseDetailsData['created_at'] = $fullCaseDetailsData['created_at'] ? MybosTime::format_mongoDbTimeUsingBuildingTimezone($fullCaseDetailsData['created_at'], $building->timezone, MybosDateTimeFormat::STANDARD) : '';
        $fullCaseDetailsData['start_8601'] = $fullCaseDetailsData['start'] ? MybosTime::UTCDateTimeToCarbon($fullCaseDetailsData['start']) : '';
        $fullCaseDetailsData['start'] = $fullCaseDetailsData['start'] ? MybosTime::format_mongoDbTimeUsingBuildingTimezone($fullCaseDetailsData['start'], $building->timezone, MybosDateTimeFormat::STANDARD) : '';
        $fullCaseDetailsData['due_8601'] = $fullCaseDetailsData['due'] ? MybosTime::UTCDateTimeToCarbon($fullCaseDetailsData['due']) : '';
        $fullCaseDetailsData['due_locale'] = $fullCaseDetailsData['due'] ? MybosTime::format_mongoDbTimeUsingBuildingTimezone($fullCaseDetailsData['due'], $localeProfile['timezone'], $localeProfile['date_format']) : '';
        $fullCaseDetailsData['due'] = $fullCaseDetailsData['due'] ? MybosTime::format_mongoDbTimeUsingBuildingTimezone($fullCaseDetailsData['due'], $building->timezone, MybosDateTimeFormat::STANDARD) : '';
        $fullCaseDetailsData['completion_date_8601'] = $fullCaseDetailsData['completion_date'] ? MybosTime::UTCDateTimeToCarbon($fullCaseDetailsData['completion_date']) : '';
        $fullCaseDetailsData['completion_date'] = $fullCaseDetailsData['completion_date'] ? MybosTime::format_mongoDbTimeUsingBuildingTimezone($fullCaseDetailsData['completion_date'], $building->timezone, MybosDateTimeFormat::AUSTRALIA_DATE_HOUR_MIN) : '';

        foreach ($fullCaseDetailsData['emails'] as $caseEmails) {
            $caseEmails['created_at_8601'] = !empty($caseEmails['created_at']) ? MybosTime::UTCDateTimeToCarbon($caseEmails['created_at']) : '';
            $caseEmails['created_at'] = !empty($caseEmails['created_at']) ? MybosTime::format_mongoDbTimeUsingBuildingTimezone($caseEmails['created_at'], $building->timezone, MybosDateTimeFormat::STANDARD) : '';
            $caseEmails['updated_at_8601'] = !empty($caseEmails['updated_at']) ? MybosTime::UTCDateTimeToCarbon($caseEmails['updated_at']) : '';
            $caseEmails['updated_at'] = !empty($caseEmails['updated_at']) ? MybosTime::format_mongoDbTimeUsingBuildingTimezone($caseEmails['updated_at'], $building->timezone, MybosDateTimeFormat::STANDARD) : '';
        }
        foreach ($fullCaseDetailsData['logs'] as $caseLogs) {
            $caseLogs['created_8601'] = MybosTime::UTCDateTimeToCarbon($caseLogs['created_at']);
            $caseLogs['created_at'] = MybosTime::format_mongoDbTimeUsingBuildingTimezone($caseLogs['created_at'], $building->timezone, MybosDateTimeFormat::AUSTRALIA_DATE_HOUR_MIN_AMPM);
        }
        if (isset($fullCaseDetailsData['photos'])) {
            foreach ($fullCaseDetailsData['photos'] as $photo) {
                if (!empty($photo['file']['created_at'])) {
                    $photo['file']['created_8601'] = MybosTime::UTCDateTimeToCarbon($photo['file']['created_at']);
                    $photo['file']['created_at'] = MybosTime::format_mongoDbTimeUsingBuildingTimezone($photo['file']['created_at'], $building->timezone, MybosDateTimeFormat::STANDARD);
                }
            }
        }
        if (isset($fullCaseDetailsData['documents'])) {
            foreach ($fullCaseDetailsData['documents'] as $doc) {
                if (!empty($doc['file']['created_at'])) {
                    $doc['file']['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($doc['file']['created_at']);
                    $doc['file']['created_at'] = MybosTime::format_mongoDbTimeUsingBuildingTimezone($doc['file']['created_at'], $building->timezone, MybosDateTimeFormat::AUSTRALIA_DATE);
                }
            }
        }
        if (isset($fullCaseDetailsData['quotes'])) {
            foreach ($fullCaseDetailsData['quotes'] as $quote) {
                $quote['created_at_8601'] = !empty($quote['created_at']) ? MybosTime::UTCDateTimeToCarbon($quote['created_at']) : '';
                $quote['created_at'] = !empty($quote['created_at']) ? MybosTime::format_mongoDbTimeUsingBuildingTimezone($quote['created_at'], $building->timezone, MybosDateTimeFormat::STANDARD) : '';
                if (!empty($quote['file']) && !empty($quote['file']['created_at'])) {
                    $quote['file']['created_at_8601'] = !empty($quote['file']['created_at']) ? MybosTime::UTCDateTimeToCarbon($quote['file']['created_at']) : '';
                    $quote['file']['created_at'] = !empty($quote['file']['created_at']) ? MybosTime::format_mongoDbTimeUsingBuildingTimezone($quote['file']['created_at'], $building->timezone, MybosDateTimeFormat::STANDARD) : '';
                }
            }
        }
        if (isset($fullCaseDetailsData['inventory_usages'])) {
            foreach ($fullCaseDetailsData['inventory_usages'] as $inventoryUsage) {
                $inventoryUsage['created_at_8601'] = !empty($inventoryUsage['created_at']) ? MybosTime::UTCDateTimeToCarbon($inventoryUsage['created_at']) : '';
                $inventoryUsage['created_at'] = !empty($inventoryUsage['created_at']) ? MybosTime::format_mongoDbTimeUsingBuildingTimezone($inventoryUsage['created_at'], $building->timezone, MybosDateTimeFormat::STANDARD) : '';
                $inventoryUsage['updated_at_8601'] = !empty($inventoryUsage['updated_at']) ? MybosTime::UTCDateTimeToCarbon($inventoryUsage['updated_at']) : '';
                $inventoryUsage['updated_at'] = !empty($inventoryUsage['updated_at']) ? MybosTime::format_mongoDbTimeUsingBuildingTimezone($inventoryUsage['updated_at'], $building->timezone, MybosDateTimeFormat::STANDARD) : '';
            }
        }

        /** No need to return as we have accepted the input array to be passed as reference... **/
    }

    private static function getSecureFileUrls(&$fullCaseDetailsData)
    {
        $s3Bucket = new AwsS3();

        foreach ($fullCaseDetailsData['emails'] as $email) {
            if (count($email['attachments']) > 0) {
                $email['attachments'][0]['secure_url'] = $s3Bucket->preSecureDocumentUrl($email['attachments'][0]['s3key']);
            }
        }

        if (!empty($fullCaseDetailsData['photos'])) {
            foreach ($fullCaseDetailsData['photos'] as $photo) {
                $s3Bucket->appendSecureUrl($photo['file']);
            }
        }

        if (!empty($fullCaseDetailsData['documents'])) {
            foreach ($fullCaseDetailsData['documents'] as $doc) {
                $s3Bucket->appendSecureUrl($doc['file']);
            }
        }

        if (!empty($fullCaseDetailsData['quotes'])) {
            foreach ($fullCaseDetailsData['quotes'] as $quote) {
                $s3Bucket->appendSecureUrl($quote['file']);
            }
        }

        if (!empty($fullCaseDetailsData['invoices'])) {
            foreach ($fullCaseDetailsData['invoices'] as $invoice) {
                $s3Bucket->appendSecureUrl($invoice['file']);
            }
        }

        /** No need to return as we have accepted the input array to be passed as reference... **/
    }

    /** NOTE: Move this out of here and into the Inventory Repository. **/
    public static function findInventoryBy($field, $fieldValue)
    {
        //return $this
    }

    /**
     * @param Building $building
     * @param string $summaryStatus
     * @param array $options
     * @return int
     */
    public static function getSummaryStatusCount(Building $building, string $summaryStatus, array $options = ['contractor_id' => '', 'contractor_ids' => []]): int
    {
        // Get all case statuses considered as specified in the [$summaryStatus].
        $caseStatus = null;
        if ($summaryStatus === MybosBaseCaseStatus::COMPLETED) {
            $caseStatus = Category::where('building_id', (string)$building['_id'])
                ->where('type', MybosCategoryType::CASE_STATUS)->where('name', MybosBaseCaseStatus::COMPLETED)->first();
        } else if ($summaryStatus === MybosBaseCaseStatus::DELETED) {
            $caseStatus = Category::where('building_id', (string)$building['_id'])
                ->where('type', MybosCategoryType::CASE_STATUS)->where('name', MybosBaseCaseStatus::DELETED)->first();
        }

        // Validate case statuses.
        if (!$caseStatus instanceof Category) {
            Log::error(__NAMESPACE__ . '\\' . __FUNCTION__ . '(): ' . 'Cannot find building\'s "' . $summaryStatus . '" status.');
            return 0;
        }

        // Initialise MongoDb client and collection.
        $mongoDb = app('MongoDbClient');
        $casesCollection = $mongoDb->getCollection('cases');

        // Get all cases from building.
        $mdbPipeline = [
            [
                '$match' => [
                    'building_id' => (string)$building['_id'],
                    'status_id' => $caseStatus['_id'],
                    'deleted_at' => null,
                ]
            ]
        ];

        // Add [contractor_id] filter if specified.
        if (!empty($options['contractor_id'])) {
            $mdbPipeline[] = [
                '$match' => [
                    'contractors.contractor_id' => $options['contractor_id'],
                ]
            ];
        }

        // Add [contractor_ids] filter if specified.
        if (!empty($options['contractor_ids'])) {
            $mdbPipeline[] = [
                '$match' => [
                    'contractors.contractor_id' => [
                        '$in' => $options['contractor_ids']
                    ]
                ]
            ];
        }

        /** Removed $lookup and $unwind pipelines here for a more optimized query... -Lino **/

        // Add $count stage to get total number of rows. (Note: This will drop all rows, similar to GroupBy when in SQL.)
        $mdbPipeline[] = [
            '$count' => 'total'
        ];

        /** Send query to mongodb. **/
        try {
            $aggregateResult = $casesCollection->aggregate($mdbPipeline);
            $summaryStatusCount = $aggregateResult->toArray();
        } catch (\Exception $e) {
            Log::error(__NAMESPACE__ . '\\' . __FUNCTION__ . "(): " . $e->getMessage());
            Log::error(__NAMESPACE__ . '\\' . __FUNCTION__ . "(): (all)", (array)$e);
            return 0;
        }

        // Return int value.
        if (empty($summaryStatusCount)) {
            return 0;
        }
        return (int)$summaryStatusCount[0]['total'];
    }

    /**
     * @param Building $building
     * @param array $options
     * @return int
     */
    public static function getSummaryCurrentCount(Building $building, array $options = ['contractor_id' => '', 'contractor_ids' => []]): int
    {
        // Get all case statuses considered as [current].
        $caseStatusDraft = null;
        $caseStatusCompleted = Category::where('building_id', (string)$building['_id'])
            ->where('type', MybosCategoryType::CASE_STATUS)->where('name', MybosBaseCaseStatus::COMPLETED)->first();
        $caseStatusDeleted = Category::where('building_id', (string)$building['_id'])
            ->where('type', MybosCategoryType::CASE_STATUS)->where('name', MybosBaseCaseStatus::DELETED)->first();

        // Validate case statuses.
        if (!$caseStatusCompleted instanceof Category || !$caseStatusDeleted instanceof Category) {
            Log::error(__NAMESPACE__ . '\\' . __FUNCTION__ . '(): ' . 'Cannot find building\'s "deleted"/"completed" statuses.');
            return 0;
        }

        // Initialise MongoDb client and collection.
        $mongoDb = app('MongoDbClient');
        $casesCollection = $mongoDb->getCollection('cases');

        // Get all cases from building.
        $mdbPipeline = [
            [
                '$match' => [
                    'building_id' => (string)$building['_id'],
                    'status_id' => [
                        '$nin' => [$caseStatusCompleted['_id'], $caseStatusDeleted['_id'], $caseStatusDraft]
                    ],
                    'deleted_at' => null,
                ]
            ]
        ];

        // Add [contractor_id] filter if specified.
        if (!empty($options['contractor_id'])) {
            $mdbPipeline[] = [
                '$match' => [
                    'contractors.contractor_id' => $options['contractor_id'],
                ]
            ];
        }

        // Add [contractor_ids] filter if specified.
        if (!empty($options['contractor_ids'])) {
            $mdbPipeline[] = [
                '$match' => [
                    'contractors.contractor_id' => [
                        '$in' => $options['contractor_ids']
                    ]
                ]
            ];
        }

        // Add $count stage to get total number of rows. (Note: This will drop all rows, similar to GroupBy when in SQL.)
        $mdbPipeline[] = [
            '$count' => 'total'
        ];

        /** Send query to mongodb. **/
        try {
            $aggregateResult = $casesCollection->aggregate($mdbPipeline);
            $summaryStatusCount = $aggregateResult->toArray();
        } catch (\Exception $e) {
            Log::error(__NAMESPACE__ . '\\' . __FUNCTION__ . "(): " . $e->getMessage());
            Log::error(__NAMESPACE__ . '\\' . __FUNCTION__ . "(): (all)", (array)$e);
            return 0;
        }

        // Return int value.
        if (empty($summaryStatusCount)) {
            return 0;
        }
        return (int)$summaryStatusCount[0]['total'];
    }

    public static function getSummaryAllCount(string $buildingId)
    {
        $mongoDb = app('MongoDbClient');
        $casesCollection = $mongoDb->getCollection('cases');
        $aggregateResult = $casesCollection->aggregate([
            [
                // Get all cases from building.
                '$match' => [
                    'deleted_at' => null,
                    'building_id' => $buildingId
                ]
            ],
            [
                // Get and add as new column, 'status', from linked categories('case status' typed).
                '$lookup' => [
                    'from' => 'categories',
                    'let' => [
                        'statusId' => ['$toObjectId' => '$status_id']
                    ],
                    'pipeline' => [
                        [
                            '$match' => [
                                '$expr' => [
                                    '$eq' => ['$$statusId', '$_id']
                                ],
                            ]
                        ]
                    ],
                    'as' => 'status'
                ]
            ],
            [
                // Get rows will not null [$status] fields.
                '$unwind' => [
                    'path' => '$status',
                    'preserveNullAndEmptyArrays' => false   // note: If set this to 'false', this will still show null [status] rows.
                ]
            ],
            [
                // Get total number of rows. (Note: This will drop all rows, similar to SQL's GroupBy.)
                '$count' => 'total'
            ]
        ]);
        $summaryStatusCount = $aggregateResult->toArray();
        if (empty($summaryStatusCount)) {
            return 0;
        }
        return $summaryStatusCount[0]['total'];
    }

    public static function getSummaryFoldersCount($folder_id, $building)
    {
        $mongoDb = app('MongoDbClient');
        $casesCollection = $mongoDb->getCollection('cases');
        $aggregateResult = $casesCollection->aggregate([
            [
                '$match' => [
                    'building_id' => [
                        '$eq' => $building->_id,
                    ],
                    'folders' => [
                        '$in' => [new ObjectId($folder_id)]
                    ],
                    'status_id' => [
                        '$ne' => null
                    ]
                ]
            ],
            [
                '$count' => 'total'
            ]
        ]);

        $summaryCase = $aggregateResult->toArray();
        if (empty($summaryCase)) {
            return 0;
        }
        return $summaryCase[0]['total'];
    }

    /**
     * @param Building $building
     * @param array $options
     * @return int
     */
    public static function getDraftCount(Building $building, array $options = ['contractor_id' => '', 'contractor_ids' => []]): int
    {
        // Initialise MongoDb client and collection.
        $mongoDb = app('MongoDbClient');
        $casesCollection = $mongoDb->getCollection('cases');

        // Get all cases from building.
        $mdbPipeline = [
            [
                '$match' => [
                    'deleted_at' => null,
                    'building_id' => (string)$building['_id'],
                    'status_id' => null
                ]
            ]
        ];

        // Add [contractor_id] filter if specified.
        if (!empty($options['contractor_id'])) {
            $mdbPipeline[] = [
                '$match' => [
                    'contractors.contractor_id' => $options['contractor_id'],
                ]
            ];
        }

        // Add [contractor_ids] filter if specified.
        if (!empty($options['contractor_ids'])) {
            $mdbPipeline[] = [
                '$match' => [
                    'contractors.contractor_id' => [
                        '$in' => $options['contractor_ids']
                    ]
                ]
            ];
        }

        // Add $count stage to get total number of rows. (Note: This will drop all rows, similar to GroupBy when in SQL.)
        $mdbPipeline[] = [
            '$count' => 'total'
        ];

        /** Send query to mongodb. **/
        try {
            $aggregateResult = $casesCollection->aggregate($mdbPipeline);
            $summaryStatusCount = $aggregateResult->toArray();
        } catch (\Exception $e) {
            Log::error(__NAMESPACE__ . '\\' . __FUNCTION__ . "(): " . $e->getMessage());
            Log::error(__NAMESPACE__ . '\\' . __FUNCTION__ . "(): (all)", (array)$e);
            return 0;
        }

        // Return int value.
        if (empty($summaryStatusCount)) {
            return 0;
        }
        return (int)$summaryStatusCount[0]['total'];
    }

    /**
     * @param Building $building
     * @param array $options
     * @return int
     */
    public static function getStarredCount(Building $building, array $options = ['contractor_id' => '', 'contractor_ids' => []]): int
    {
        // Initialise MongoDb client and collection.
        $mongoDb = app('MongoDbClient');
        $casesCollection = $mongoDb->getCollection('cases');

        // Get all cases from building.
        $mdbPipeline = [
            [
                '$match' => [
                    'deleted_at' => null,
                    'building_id' => (string)$building['_id'],
                    'starred' => 1
                ]
            ]
        ];

        // Add [contractor_id] filter if specified.
        if (!empty($options['contractor_id'])) {
            $mdbPipeline[] = [
                '$match' => [
                    'contractors.contractor_id' => $options['contractor_id'],
                ]
            ];
        }

        // Add [contractor_ids] filter if specified.
        if (!empty($options['contractor_ids'])) {
            $mdbPipeline[] = [
                '$match' => [
                    'contractors.contractor_id' => [
                        '$in' => $options['contractor_ids']
                    ]
                ]
            ];
        }

        // Add $count stage to get total number of rows. (Note: This will drop all rows, similar to GroupBy when in SQL.)
        $mdbPipeline[] = [
            '$count' => 'total'
        ];

        /** Send query to mongodb. **/
        try {
            $aggregateResult = $casesCollection->aggregate($mdbPipeline);
            $summaryStatusCount = $aggregateResult->toArray();
        } catch (\Exception $e) {
            Log::error(__NAMESPACE__ . '\\' . __FUNCTION__ . "(): " . $e->getMessage());
            Log::error(__NAMESPACE__ . '\\' . __FUNCTION__ . "(): (all)", (array)$e);
            return 0;
        }

        // Return int value.
        if (empty($summaryStatusCount)) {
            return 0;
        }
        return (int)$summaryStatusCount[0]['total'];
    }

    /**
     * @param Building $building
     * @param array $options
     * @return int
     */
    public static function getOverdueCount(Building $building, array $options = ['contractor_id' => '', 'contractor_ids' => []]): int
    {
        // Initialise MongoDb client and collection.
        $mongoDb = app('MongoDbClient');
        $casesCollection = $mongoDb->getCollection('cases');

        $isoNow = MybosTime::dbCompatible(MybosTime::now());
        $caseStatusCompleted = $building->getCaseStatusBy('name', MybosBaseCaseStatus::COMPLETED);
        $caseStatusDeleted = $building->getCaseStatusBy('name', MybosBaseCaseStatus::DELETED);

        if (!$caseStatusCompleted instanceof Category || !$caseStatusDeleted instanceof Category) {
            Log::error(__NAMESPACE__ . '\\' . __FUNCTION__ . '(): ' . 'Cannot find building\'s "deleted"/"completed" statuses.');
            return 0;
        }

        // Get all cases from building.
        $mdbPipeline = [
            [
                '$match' => [
                    'building_id' => (string)$building['_id'],
                    'status_id' => [
                        '$nin' => [$caseStatusCompleted['_id'], $caseStatusDeleted['_id']]
                    ],
                    'due' => ['$lt' => $isoNow],
                    'deleted_at' => null,
                ]
            ]
        ];


        // Add [contractor_id] filter if specified.
        if (!empty($options['contractor_id'])) {
            $mdbPipeline[] = [
                '$match' => [
                    'contractors.contractor_id' => $options['contractor_id'],
                ]
            ];
        }

        // Add [contractor_ids] filter if specified.
        if (!empty($options['contractor_ids'])) {
            $mdbPipeline[] = [
                '$match' => [
                    'contractors.contractor_id' => [
                        '$in' => $options['contractor_ids']
                    ]
                ]
            ];
        }

        // Add $count stage to get total number of rows. (Note: This will drop all rows, similar to GroupBy when in SQL.)
        $mdbPipeline[] = [
            '$count' => 'total'
        ];

        /** Send query to mongodb. **/
        try {
            $aggregateResult = $casesCollection->aggregate($mdbPipeline);
            $summaryStatusCount = $aggregateResult->toArray();
        } catch (\Exception $e) {
            Log::error(__NAMESPACE__ . '\\' . __FUNCTION__ . "(): " . $e->getMessage());
            Log::error(__NAMESPACE__ . '\\' . __FUNCTION__ . "(): (all)", (array)$e);
            return 0;
        }

        // Return int value.
        if (empty($summaryStatusCount)) {
            return 0;
        }
        return (int)$summaryStatusCount[0]['total'];
    }

    /**
     * Optimized paginated [cases] list.
     * Note: We are using a 2-step pagination here to separately get pagination info and page-items
     *       details to ensure that we only do $lookups for specific number of items per page.
     *      1.  Search cases without any $lookups.
     *      2.  Get specific page-items details (with $lookups).
     *
     * @param CasesListFilter $filter
     * @param Building $building
     * @return array
     */
    public static function paginatedCasesList(CasesListFilter $filter, Building $building): array
    {
        /** Get pagination info(with no/minimal $lookups). **/
        $pagedCasesInfo = self::getCasesListPaginationInfo($filter, $building);

        // Get case IDs from paginated result data.
        $caseIds = collect($pagedCasesInfo['data'])->map(function ($eachCase) {
            return $eachCase['_id'];
        })->toArray();

        /** Get specific page-items details(with the necessary $lookups). **/
        $currentPageCasesDetails = self::getCasesDetails($caseIds, $filter, $building);

        // Overwrite paginated cases-data with detailed cases-data.
        $pagedCasesInfo['data'] = $currentPageCasesDetails;

        // Return result.
        return $pagedCasesInfo;
    }

    /**
     * @param CasesListFilter $filter
     * @param Building $building
     * @return array
     */
    public static function paginatedCasesListDetails(CasesListFilter $filter, Building $building): array
    {
        /** Get pagination info(with no/minimal $lookups). **/
        $pagedCasesInfo = self::getCasesListPaginationInfo($filter, $building);

        // Get case IDs from paginated result data.
        $caseIds = collect($pagedCasesInfo['data'])->map(function ($eachCase) {
            return new ObjectId($eachCase['_id']);
        })->toArray();

        /** Get specific page-items details(with the necessary $lookups). **/
        $currentPageCasesDetails = self::getCasesListFullDetails($caseIds, $building);

        foreach ($currentPageCasesDetails as &$eachCase){

            $eachCase['history_note_format'] = str_replace("<br />", "\n", $eachCase['history_note']);
            /** format sent date... **/
            foreach ($eachCase['emails'] as &$email) {
                $email['sent_at_8601'] = MybosTime::UTCDateTimeToCarbon($email['created_at']);
                $email['sent_at'] = MybosTime::format_mongoDbTimeUsingBuildingTimezone($email['created_at'], $building['timezone'], 'd/m/Y h:iA');
            }

            /** Convert dates to actual building's timezone... **/
            self::convertDatesToBuildingTimezone($eachCase, $building);

            /** Get pre-secured url for all images/files... **/
            self::getSecureFileUrls($eachCase);

            /** Get case's detailed contractor data... **/
            $eachCase['contractors'] = self::getDetailedContractorsWithDocuments((array)$eachCase['contractors'], $building);
        }

        // Overwrite paginated cases-data with detailed cases-data.
        $pagedCasesInfo['data'] = $currentPageCasesDetails;

        // Return result.
        return $pagedCasesInfo;
    }


    /**
     * Search cases without/minimal $lookups.
     *
     * @param CasesListFilter $filter
     * @param Building $building
     * @return array
     */
    public static function getCasesListPaginationInfo(CasesListFilter $filter, Building $building): array
    {
        $mongoDb = app('MongoDbClient');
        $casesCollection = $mongoDb->getCollection('cases');
        $mdbPipeline = [];

        // Sanitize filters, although this has already been cleaned, lets sanitize it again anyway...
        $filterCaseIds = $filter->_ids ?? [];
        $filterContractorIds = $filter->contractors ?? [];
        $filterApartmentIds = $filter->apartment ?? [];
        $filterTypeIds = $filter->type ?? [];
        $filterAssetId = $filter->asset_id ?? '';
        $filterFolderId = $filter->folder ?? '';
        $filterStatus = $filter->status ?? '';
        $filterDue = $filter->due ?? '';
        $filterPriority = $filter->priority ?? '';
        $filterDaysOpen = $filter->days_open ?? '';
        $filterStartDate = $filter->start_date ?? '';
        $filterEndDate = $filter->end_date ?? '';
        $filterKeyword = $filter->keyword ?? '';
        $filterCreatedMinDate = $filter->created_at_min ?? '';
        $filterCreatedMaxDate = $filter->created_at_max ?? '';
        $filterUpdatedMinDate = $filter->updated_at_min ?? '';
        $filterUpdatedMaxDate = $filter->updated_at_max ?? '';

        /** Gather initial $match filter(s). **/
        $mdbMatchStage = [
            '$or' => [
                ['building_id' => $building['_id']],
                [
                    /** Also get building synced cases **/
                    'duplicate_sync_building_ids' => [
                        '$in' => [$building['_id']]
                    ]
                ]
            ],
            'deleted_at' => null,
        ];

        // Add specific [case-IDs].
        if (!empty($filterCaseIds)) {
            $objectID = [];
            foreach ($filterCaseIds as $_id) {
                $objectID[] = new ObjectId($_id);
            }
            $mdbMatchStage['_id'] = ['$in' => $objectID];
        }

        // Add [case-folder] filter if specified, otherwise use the [case-status] and fallback finally at CURRENT case-status.
        if (!empty($filterFolderId)) {
            $mdbMatchStage['folders'] = [
                '$in' => [new ObjectId($filterFolderId)]
            ];
        }
        else {
            // Check case status filter.
            if ($filterStatus === MybosCaseSummaryItem::COMPLETED) {
                $caseStatusCompleted = $building->getCaseStatusBy('name', MybosBaseCaseStatus::COMPLETED);
                $filter->status = [$caseStatusCompleted['_id']];
                $mdbMatchStage['status_id'] = ['$in' => $filter->status, '$ne' => null];
            } else if ($filterStatus === MybosCaseSummaryItem::CURRENT) {
                $caseStatusCompleted = $building->getCaseStatusBy('name', MybosBaseCaseStatus::COMPLETED);
                $caseStatusDeleted = $building->getCaseStatusBy('name', MybosBaseCaseStatus::DELETED);
                $filter->status = [$caseStatusCompleted->_id ?? '', $caseStatusDeleted->_id ?? ''];
                $mdbMatchStage['status_id'] = ['$nin' => $filter->status, '$ne' => null];
            } else if ($filterStatus === MybosCaseSummaryItem::TRASH) {
                $caseStatusDeleted = $building->getCaseStatusBy('name', MybosBaseCaseStatus::DELETED);
                $filter->status = [$caseStatusDeleted['_id']];
                $mdbMatchStage['status_id'] = ['$in' => $filter->status, '$ne' => null];
            } else if ($filterStatus === MybosCaseSummaryItem::STARRED) {
                $mdbMatchStage['starred'] = 1;
            } else if ($filterStatus === MybosCaseSummaryItem::OVERDUE) {
                $caseStatusCompleted = $building->getCaseStatusBy('name', MybosBaseCaseStatus::COMPLETED);
                $caseStatusDeleted = $building->getCaseStatusBy('name', MybosBaseCaseStatus::DELETED);
                $carbonNow = MybosTime::now();
                $isoNow = MybosTime::dbCompatible($carbonNow);
                $mdbMatchStage['due'] = ['$lt' => $isoNow];
                $mdbMatchStage['status_id'] = ['$nin' => [$caseStatusCompleted['_id'], $caseStatusDeleted['_id']]];
            } else if ($filterStatus === MybosCaseSummaryItem::DRAFT) {
                $mdbMatchStage['status_id'] = ['$eq' => null];
            } else {
                // $filterStatus === MybosCaseSummaryItem::CURRENT
                if (!empty($filterStatus)) {
                    $caseStatus = Category::select('_id')
                        ->where('building_id', $building['_id'])
                        ->where('deleted_at', null)
                        ->where('type', MybosCategoryType::CASE_STATUS)->get()->toArray();
                    $filter->status = array_column($caseStatus, '_id');
                } else {
                    $filter->status = [];
                }
                if(!empty($filter->status)){
                    $mdbMatchStage['status_id'] = ['$in' => $filter->status, '$ne' => null];
                };
            }
        }

        // Add [case-type] filter.
        if (!empty($filterTypeIds)) {
            $mdbMatchStage['type_id'] = ['$in' => $filterTypeIds];
        }

        // Add [priority] filter.
        if (!empty($filterPriority)) {
            $mdbMatchStage['priority_id'] = $filterPriority;
        }

        // Add [apartments] filter.     // Todo: confirm this filter(apartments)... -Lino
        if (!empty($filterApartmentIds)) {
            $apartmentObjectIds = [];
            foreach ($filterApartmentIds as $apartment) {
                $apartmentObjectIds[] = new ObjectId($apartment);
            }
            $mdbMatchStage['apartments'] = ['$in' => $apartmentObjectIds];
        }

        // Add [contractors] filter.    // Todo: validate string IDs... -Lino
        if (!empty($filterContractorIds)) {
            $mdbMatchStage['contractors.contractor_id'] = ['$in' => $filterContractorIds];
        }

        // Add [assets] filter.
        if (!empty($filterAssetId)) {
            $mdbMatchStage['assets'] = ['$in' => [new ObjectId($filterAssetId)]];
        }

        // Add [due] filter.
        if (!empty($filterDue)) {
            $carbonNow = MybosTime::now();
            $isoEndOfThisWeek = MybosTime::dbCompatible($carbonNow->endOfWeek(CarbonInterface::SATURDAY));
            if ($filterDue == 'current_week') {
                $dueFilter = ['$lt' => $isoEndOfThisWeek];
            } else if ($filterDue == 'next_week') {
                $carbonNextWeek = MybosTime::now()->addWeeks(1);
                $isoEndOfNextWeek = MybosTime::dbCompatible($carbonNextWeek->endOfWeek(CarbonInterface::SATURDAY));
                $dueFilter = ['$gt' => $isoEndOfThisWeek, '$lt' => $isoEndOfNextWeek];
            } else {    // $filter->due == 'overdue'
                $isoNow = MybosTime::dbCompatible($carbonNow);
                $dueFilter = ['$lt' => $isoNow];
            }
            $mdbMatchStage['due'] = $dueFilter;
        }

        // Add [days_open] filter.
        if (!empty($filterDaysOpen)) {
            $daysOpenFilter = [
                '$gt' => [
                    MongoDbNative::_safeDateDifference('$start', new UTCDateTime(), 'day', ''),
                    $filterDaysOpen
                ]
            ];
            $mdbMatchStage['$expr'] = $daysOpenFilter;
        }

        // Add [start - end] date filter.
        if (!empty($filterStartDate) && !empty($filterEndDate)) {
            $start_date = MybosTime::CarbonToUTCDateTime(Carbon::createFromFormat('Y-m-d H:i:s', $filterStartDate . ' 00:00:00', $building['timezone']));
            $end_date = MybosTime::CarbonToUTCDateTime(Carbon::createFromFormat('Y-m-d H:i:s', $filterEndDate . ' 23:59:59', $building['timezone']));
            $mdbMatchStage['$and'] = [
                ['start' => ['$gte' => $start_date]],
                ['start' => ['$lte' => $end_date]]
            ];
        }

        if (!empty($filterCreatedMinDate) && !empty($filterCreatedMaxDate)) {
            $create_at_min = MybosTime::CarbonToUTCDateTime(Carbon::make($filterCreatedMinDate)->setTimezone('utc'));
            $create_at_max = MybosTime::CarbonToUTCDateTime(Carbon::make($filterCreatedMaxDate)->setTimezone('utc'));
            $mdbMatchStage['$and'] = [
                ['created_at' => ['$gte' => $create_at_min]],
                ['created_at' => ['$lte' => $create_at_max]]
            ];
        }

        if (!empty($filterUpdatedMinDate) && !empty($filterUpdatedMaxDate)) {
            $update_at_min = MybosTime::CarbonToUTCDateTime(Carbon::make($filterUpdatedMinDate)->setTimezone('utc'));
            $update_at_max = MybosTime::CarbonToUTCDateTime(Carbon::make($filterUpdatedMaxDate)->setTimezone('utc'));
            $mdbMatchStage['$and'] = [
                ['updated_at' => ['$gte' => $update_at_min]],
                ['updated_at' => ['$lte' => $update_at_max]]
            ];
        }

        /** Add $match filters as the initial stage in the pipeline. **/
        $mdbPipeline[] = ['$match' => $mdbMatchStage];

        /** Add filters requiring $lookups. **/
        if (!empty($filterKeyword) || !empty($filter->sort_by)) {
            $mdbPipeline = MQLCases::_buildCasesSearchFilterStage($filterKeyword, $mdbPipeline);
        }

        // Assemble sort details.
        $sortSpec = ['_id' => 1];       // Note: Can also be multiple sorted: ex: ['subject' => 1, 'number' => 1].
        if (!empty($filter->sort_by)) {

            // Todo: Fix sort value on both category and case-priority edit before uncommenting this... -Lino
            //// Set to priority sort number when sorting is by "case priority"
            //if ($filter->sort_by === 'case_priority_name') {
            //    $filter->sort_by = 'case_priority_sort';
            //}

            // Set to created_at when sorting is by "case draft" status
            if ($filter->sort_by === 'number' && $filter->status === MybosCaseSummaryItem::DRAFT) {
                $filter->sort_by = 'created_at';
            }

            $sortSpec = [$filter->sort_by => 1];
            if ($filter->sort_mode === 'desc') {
                $sortSpec = [$filter->sort_by => -1];
            }
        }

        // Return paginated [cases] data.
        return $mongoDb->paginatedAggregationSearch(
            ['pageSize' => $filter->limit, 'pageNumber' => $filter->page, 'sort' => $sortSpec],
            $casesCollection, $mdbPipeline
        );
    }

    /**
     * @param array $caseIds
     * @param CasesListFilter $filter
     * @param Building $building
     * @return array
     */
    public static function getCasesDetails(array $caseIds, CasesListFilter $filter, Building $building): array
    {
        $mongoDb = app('MongoDbClient');
        $casesCollection = $mongoDb->getCollection('cases');

        // Ensure case-ids are in ObjectId format.
        $caseObjIds = array_map(static function ($eachItem) {
            return new ObjectId($eachItem);
        }, $caseIds);

        /** Only match filtered and paginated cases IDs here. **/
        $mdbStage1Pipeline = [
            '_id' => [
                '$in' => $caseObjIds
            ]
        ];

        // Assemble query pipelines.
        $mdbPipeline = [
            ['$match' => $mdbStage1Pipeline]
        ];

        $mdbStage2Pipeline = [
            '$addFields' => [
                '_id_str' => [
                    // Get string converted _id.
                    '$toString' => '$_id'
                ],
                '_id_type_id' =>
                // Get ObjectId converted type_id.
                    MongoDbNative::_convertToNullOnError('$type_id', 'objectId'),
                '_id_priority_id' =>
                // Get ObjectId converted type_id.
                    MongoDbNative::_convertToNullOnError('$priority_id', 'objectId'),
                '_id_status_id' =>
                // Get ObjectId converted type_id.
                    MongoDbNative::_convertToNullOnError('$status_id', 'objectId'),
            ]
        ];

        // Case Types [Categories]
        $mdbStage3aPipeline = [
            '$lookup' => [
                'from' => 'categories',
                'localField' => '_id_type_id',
                'foreignField' => '_id',
                'pipeline' => [
                    [
                        '$project' => ['name' => 1, '_id' => 0]
                    ]
                ],
                'as' => 'case_type',
            ]
        ];
        $mdbStage3bPipeline = [
            '$unwind' => [
                'path' => '$case_type',
                'preserveNullAndEmptyArrays' => true
            ]
        ];
        $mdbStage3cPipeline = [
            '$addFields' => [
                'case_type_name' => '$case_type.name',
            ]
        ];

        // Case Priorities [Categories]
        $mdbStage3a2Pipeline = [
            '$lookup' => [
                'from' => 'categories',
                'localField' => '_id_priority_id',
                'foreignField' => '_id',
                'pipeline' => [
                    [
                        '$project' => ['name' => 1, 'sort' => 1, '_id' => 0]
                    ]
                ],
                'as' => 'case_priority',
            ]
        ];
        $mdbStage3b2Pipeline = [
            '$unwind' => [
                'path' => '$case_priority',
                'preserveNullAndEmptyArrays' => true
            ]
        ];
        $mdbStage3c2Pipeline = [
            '$addFields' => [
                'case_priority_sort' => '$case_priority.sort',
                'case_priority_name' => '$case_priority.name',
            ]
        ];

        // Contractors
        $mdbStage4aPipeline = [
            '$addFields' => [
                'contractor_w_objectIds' => [
                    '$map' => [
                        'input' => '$contractors',
                        'in' => [
                            '$mergeObjects' => [
                                '$$this',
                                [
                                    'contractor_id' => MongoDbNative::_convertToNullOnError('$$this.contractor_id', 'objectId'),
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        ];
        $mdbStage4bPipeline = [
            '$lookup' => [
                'from' => 'contractors',
                'localField' => 'contractor_w_objectIds.contractor_id',
                'foreignField' => '_id',
                'pipeline' => [
                    [
                        '$project' => [
                            'company_name' => 1, 'website' => 1, 'phone' => 1, 'email' => 1, 'contacts' => 1
                        ]
                    ]
                ],
                'as' => 'contractor_details',
            ]
        ];
        $mdbStage4cPipeline = [
            '$addFields' => [
                'contactor_company_names' => [
                    '$reduce' => [
                        'input' => '$contractor_details',
                        'initialValue' => '',
                        'in' => [
                            '$cond' => [
                                ['$eq' => ['$$value', '']],
                                ['$concat' => ['$$value', '$$this.company_name']],
                                ['$concat' => ['$$value', ', ', '$$this.company_name']]
                            ]
                        ]
                    ]
                ]
            ]
        ];

        // Apartments
        $mdbStage5aPipeline = [
            '$lookup' => [
                'from' => 'apartments',
                'localField' => 'apartments',
                'foreignField' => '_id',
                'pipeline' => [
                    [
                        '$project' => [
                            'unit_label' => 1, 'lot' => 1, 'status' => 1
                        ]
                    ]
                ],
                'as' => 'apartment_details',
            ]
        ];
        $mdbStage5bPipeline = [
            '$addFields' => [
                'apartment_unit_labels' => [
                    '$reduce' => [
                        'input' => '$apartment_details',
                        'initialValue' => '',
                        'in' => [
                            '$cond' => [
                                ['$eq' => ['$$value', '']],
                                ['$concat' => ['$$value', '$$this.unit_label']],
                                ['$concat' => ['$$value', ', ', '$$this.unit_label']]
                            ]
                        ]
                    ]
                ]
            ]
        ];

        // Status
        $mdbStage6aPipeline = [
            '$lookup' => [
                'from' => 'categories',
                'localField' => '_id_status_id',
                'foreignField' => '_id',
                'pipeline' => [
                    [
                        '$project' => ['name' => 1, '_id' => 0]
                    ]
                ],
                'as' => 'case_status',
            ]
        ];
        $mdbStage6bPipeline = [
            '$unwind' => [
                'path' => '$case_status',
                'preserveNullAndEmptyArrays' => true
            ]
        ];
        $mdbStage6cPipeline = [
            '$addFields' => [
                'case_status_name' => '$case_status.name',
            ]
        ];

        $mdbStage8aPipeline = [
            '$addFields' => [
                'job_area_name' => [
                    '$reduce' => [
                        'input' => '$area',
                        'initialValue' => '',
                        'in' => [
                            '$cond' => [
                                ['$eq' => ['$$value', '']],
                                ['$concat' => ['$$value', '$$this']],
                                ['$concat' => ['$$value', ', ', '$$this']]
                            ]
                        ]
                    ]
                ]
            ]
        ];

        // Assets
        $mdbStage9aPipeline = [
            '$lookup' => [
                'from' => 'assets',
                'localField' => 'assets',
                'foreignField' => '_id',
                'pipeline' => [
                    [
                        '$project' => [
                            'name' => 1, 'description' => 1, '_id' => ['$toString' => '$_id']
                        ]
                    ]
                ],
                'as' => 'assets_details',
            ]
        ];
        $mdbStage9bPipeline = [
            '$addFields' => [
                'asset_name' => [
                    '$reduce' => [
                        'input' => '$assets_details',
                        'initialValue' => '',
                        'in' => [
                            '$cond' => [
                                ['$eq' => ['$$value', '']],
                                ['$concat' => ['$$value', '$$this.name']],
                                ['$concat' => ['$$value', ', ', '$$this.name']]
                            ]
                        ]
                    ]
                ]
            ]
        ];

        $mdbStage10aPipeline = [
            '$addFields' => [
                'case_inventory_usage' => [
                    '$map' => [
                        'input' => '$inventory_usages',
                        'in' => [
                            '$mergeObjects' => [
                                '$$this',
                                [
                                    'inventory_id' => MongoDbNative::_convertToNullOnError('$$this.inventory_id', 'objectId'),
                                ]

                            ]
                        ]
                    ]
                ]
            ]
        ];
        $mdbStage11aPipeline = [
            // [Inventory Usages s2] : lookup inventory based on [inventory_usage_details] converted contractor_id above.
            '$lookup' => [
                'from' => 'inventories',
                'localField' => 'case_inventory_usage.inventory_id',
                'foreignField' => '_id',
                'pipeline' => [
                    [
                        '$project' => [
                            'item' => 1, 'location' => 1, 'description' => 1, 'owner' => 1, 'serial' => 1,
                            'value' => 1, 'sell' => 1, 'quantity' => 1, 'unlimited' => 1,
                        ]
                    ]
                ],
                'as' => 'inventory_details',
            ]
        ];
        $mdbStage12aPipeline = [
            '$addFields' => [
                'inventory_usages' => [
                    '$map' => [
                        'input' => '$case_inventory_usage',
                        'as' => 'ciu',
                        'in' => [
                            '$mergeObjects' => [
                                '$$ciu',
                                [
                                    // Add inventory details as embedded data. (Note: Alternatively, remove this to actually do a merge instead of embedding)
                                    'inventory' => [
                                        '$first' => [
                                            '$filter' => [
                                                'input' => '$inventory_details',
                                                'cond' => [
                                                    '$eq' => ['$$this._id', '$$ciu.inventory_id']
                                                ]
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        ];
        $mdbStage13aPipeline = [
            // [Quotes s1] : convert contractor_id string to objectId
            '$addFields' => [
                'quotes_details' => [
                    '$map' => [
                        'input' => '$quotes',
                        'in' => [
                            '$mergeObjects' => [
                                '$$this',
                                [
                                    'contractor_id' => MongoDbNative::_convertToNullOnError('$$this.contractor_id', 'objectId'),
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        ];
        $mdbStage14aPipeline = [
            // [Quotes s2] : lookup contractors based on [emails_details] converted contractor_id above.
            '$lookup' => [
                'from' => 'contractors',
                'localField' => 'quotes_details.contractor_id',
                'foreignField' => '_id',
                'pipeline' => [
                    [
                        '$project' => [
                            'company_name' => 1, 'website' => 1
                        ]
                    ]
                ],
                'as' => 'quotes_contractor_details',
            ]
        ];
        $mdbStage15aPipeline = [
            // [Quotes s3] : do $addFields here to filter out more fields to remove later below on the projection.
            '$addFields' => [
                'quotes' => [
                    '$map' => [
                        'input' => '$quotes_details',
                        'as' => 'ed',
                        'in' => [
                            '$mergeObjects' => [
                                '$$ed',
                                [
                                    // Add contractor details as embedded data.  (Note: Alternatively, remove this to actually do a merge instead of embedding)
                                    'contractor' => [
                                        '$first' => [
                                            '$filter' => [
                                                'input' => '$quotes_contractor_details',
                                                'cond' => [
                                                    '$eq' => ['$$this._id', '$$ed.contractor_id']
                                                ]
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        ];
        $mdbStage16aPipeline = [
            // [Invoice s1] : convert contractor_id string to objectId
            '$addFields' => [
                'invoices_details' => [
                    '$map' => [
                        'input' => '$invoices',
                        'in' => [
                            '$mergeObjects' => [
                                '$$this',
                                [
                                    'contractor_id' => MongoDbNative::_convertToNullOnError('$$this.contractor_id', 'objectId'),
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        ];
        $mdbStage17aPipeline = [
            // [Invoices s2] : lookup contractors based on [invoices_details] converted contractor_id above.
            '$lookup' => [
                'from' => 'contractors',
                'localField' => 'invoices_details.contractor_id',
                'foreignField' => '_id',
                'pipeline' => [
                    [
                        '$project' => [
                            'company_name' => 1, 'website' => 1
                        ]
                    ]
                ],
                'as' => 'invoices_contractor_details',
            ]
        ];
        $mdbStage18aPipeline = [
            // [Quotes s3] : do $addFields here to filter out more fields to remove later below on the projection.
            '$addFields' => [
                'invoices' => [
                    '$map' => [
                        'input' => '$invoices_details',
                        'as' => 'ed',
                        'in' => [
                            '$mergeObjects' => [
                                '$$ed',
                                [
                                    // Add contractor details as embedded data.  (Note: Alternatively, remove this to actually do a merge instead of embedding)
                                    'contractor' => [
                                        '$first' => [
                                            '$filter' => [
                                                'input' => '$invoices_contractor_details',
                                                'cond' => [
                                                    '$eq' => ['$$this._id', '$$ed.contractor_id']
                                                ]
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        ];

        $mdbStage19aPipeline = [
            // [Status - Categories]
            '$lookup' => [
                'from' => 'categories',
                'let' => [
                    'statusId' => MongoDbNative::_convertToNullOnError('$status_id', 'objectId')
                ],
                'pipeline' => [
                    [
                        '$match' => [
                            '$expr' => [
                                '$eq' => ['$$statusId', '$_id']
                            ]
                        ]
                    ],
                    [
                        '$project' => [
                            'name' => 1, 'type' => 1
                        ]
                    ]
                ],
                'as' => 'status'
            ]
        ];
        $mdbStage20aPipeline = [
            // [Type - Categories]
            '$lookup' => [
                'from' => 'categories',
                'let' => [
                    'typeId' => MongoDbNative::_convertToNullOnError('$type_id', 'objectId')
                ],
                'pipeline' => [
                    [
                        '$match' => [
                            '$expr' => [
                                '$eq' => ['$$typeId', '$_id']
                            ]
                        ]
                    ],
                    [
                        '$project' => [
                            'name' => 1, 'type' => 1
                        ]
                    ]
                ],
                'as' => 'type'
            ]
        ];
        $mdbStage21aPipeline = [
            // [Priority - Categories]
            '$lookup' => [
                'from' => 'categories',
                'let' => [
                    'priorityId' => MongoDbNative::_convertToNullOnError('$priority_id', 'objectId')
                ],
                'pipeline' => [
                    [
                        '$match' => [
                            '$expr' => [
                                '$eq' => ['$$priorityId', '$_id']
                            ]
                        ]
                    ],
                    [
                        '$project' => [
                            'name' => 1, 'type' => 1
                        ]
                    ]
                ],
                'as' => 'priority'
            ]
        ];
        $mdbStage22aPipeline = [
            '$addFields' => [
                'start_8601' => MongoDbNative::_dateToString('$start', null, null),
                'due_8601' => MongoDbNative::_dateToString('$due', null, null),
                'created_at_8601' => MongoDbNative::_dateToString('$created_at', null, null),
                'completion_date_8601' => MongoDbNative::_dateToString('$completion_date', null, null),
            ]
        ];

        array_push(
            $mdbPipeline, $mdbStage2Pipeline,
            $mdbStage3aPipeline, $mdbStage3bPipeline, $mdbStage3cPipeline,
            $mdbStage3a2Pipeline, $mdbStage3b2Pipeline, $mdbStage3c2Pipeline,
            $mdbStage4aPipeline, $mdbStage4bPipeline, $mdbStage4cPipeline,
            $mdbStage5aPipeline, $mdbStage5bPipeline,
            $mdbStage6aPipeline, $mdbStage6bPipeline, $mdbStage6cPipeline,
            $mdbStage8aPipeline, $mdbStage9aPipeline, $mdbStage9bPipeline, $mdbStage10aPipeline,
            $mdbStage11aPipeline, $mdbStage12aPipeline, $mdbStage13aPipeline, $mdbStage14aPipeline,
            $mdbStage15aPipeline, $mdbStage16aPipeline, $mdbStage17aPipeline, $mdbStage18aPipeline,
            $mdbStage19aPipeline, $mdbStage20aPipeline, $mdbStage21aPipeline, $mdbStage22aPipeline
        );

        /** Ensure sort-order from paginated result is maintained. **/
        $mdbSortStageA = [
            // Add sort order as passed from paginated IDs result.
            '$addFields' => [
                'sort_order' => [
                    '$indexOfArray' => [$caseObjIds, '$_id']
                ]
            ]
        ];
        $mdbSortStageB = [
            // Sort by sort_order from paginated IDs result.
            '$sort' => ['sort_order' => 1]
        ];
        array_push($mdbPipeline, $mdbSortStageA, $mdbSortStageB);

        /** Send native query to MongoDb. **/
        $queryResult = $casesCollection->aggregate($mdbPipeline);
        $casesArray = $queryResult->toArray();

        /**
         * Todo: Ask @Peter-Acevski if we need to set default locale or respond an error here...
         **/
        //$localeProfile = $building->locale_profile->toArray();
        try {
            $localeProfile = $building->locale_profile->toArray();
        } catch (\Exception $e) {
            $localeProfile = [
                'timezone' => MybosUserSession::getBestTimezone(),
                'date_format' => 'd/m/Y'
            ];

            $systemMessage = $e->getMessage();
            Log::error(__NAMESPACE__ . '\\' . __FUNCTION__ . "(): " . $systemMessage);
            Log::error(__NAMESPACE__ . '\\' . __FUNCTION__ . "(): (all)", (array)$e);
        }

        // format dates.
        foreach ($casesArray as &$case) {
            $case['start'] = !empty($case['start']) ? MybosTime::format_mongoDbTimeUsingBuildingTimezone($case['start'], $localeProfile['timezone'], $localeProfile['date_format']) : '';
            $case['due'] = !empty($case['due']) ? MybosTime::format_mongoDbTimeUsingBuildingTimezone($case['due'], $localeProfile['timezone'], $localeProfile['date_format']) : '';
            $case['created'] = !empty($case['created_at']) ? MybosTime::format_mongoDbTimeUsingBuildingTimezone($case['created_at'], $localeProfile['timezone'], $localeProfile['date_format']) : '';
            if (!empty($case['photos'])) {
                foreach ($case['photos'] as &$photo) {
                    $photo['file']['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($photo['file']['created_at']);
                    $photo['file']['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($photo['file']['updated_at']);
                }
            }
            if (!empty($case['documents'])) {
                foreach ($case['documents'] as &$document) {
                    $document['file']['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($document['file']['created_at']);
                    $document['file']['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($document['file']['updated_at']);
                }
            }
            if (!empty($case['contractors'])) {
                foreach ($case['contractors'] as &$contractor) {
                    $contractor['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($contractor['created_at']);
                    $contractor['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($contractor['updated_at']);
                }
            }
            if (!empty($case['contractor_w_objectIds'])) {
                foreach ($case['contractor_w_objectIds'] as &$contractor_w_objectIds) {
                    $contractor_w_objectIds['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($contractor_w_objectIds['created_at']);
                    $contractor_w_objectIds['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($contractor_w_objectIds['updated_at']);
                }
            }
            if (!empty($case['emails'])) {
                foreach ($case['emails'] as &$email) {
                    $email['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($email['created_at']);
                    $email['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($email['updated_at']);
                    $email['due_by_8601'] = !empty($email['due_by']) ? MybosTime::UTCDateTimeToCarbon($email['due_by']) : null;
                }
            }
            if (!empty($case['logs'])) {
                foreach ($case['logs'] as &$log) {
                    $log['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($log['created_at']);
                    $log['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($log['updated_at']);
                }
            }
            if (!empty($case['quotes'])) {
                foreach ($case['quotes'] as &$quote) {
                    $quote['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($quote['created_at']);
                    $quote['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($quote['updated_at']);
                    if (!empty($quote['file'])) {
                        $quote['file']['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($quote['file']['created_at']);
                        $quote['file']['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($quote['file']['updated_at']);
                    }
                }
            }
            if (!empty($case['inventory_usages'])) {
                foreach ($case['inventory_usages'] as &$inventory_usage) {
                    $inventory_usage['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($inventory_usage['created_at']);
                    $inventory_usage['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($inventory_usage['updated_at']);
                }
            }
            if (!empty($case['resized_photos'])) {
                foreach ($case['resized_photos'] as &$resized_photos) {
                    $resized_photos['aws_s3']['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($resized_photos['aws_s3']['created_at']);
                    $resized_photos['aws_s3']['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($resized_photos['aws_s3']['updated_at']);
                }
            }
            if (!empty($case['invoices'])) {
                foreach ($case['invoices'] as &$invoice) {
                    $invoice['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($invoice['created_at']);
                    $invoice['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($invoice['updated_at']);
                    if (!empty($invoice['file'])) {
                        $invoice['file']['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($invoice['file']['created_at']);
                        $invoice['file']['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($invoice['file']['updated_at']);
                    }
                }
            }
            if (!empty($case['invoices_details'])) {
                foreach ($case['invoices_details'] as &$invoices_details) {
                    $invoices_details['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($invoices_details['created_at']);
                    $invoices_details['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($invoices_details['updated_at']);
                    if (!empty($invoices_details['file'])) {
                        $invoices_details['file']['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($invoices_details['file']['created_at']);
                        $invoices_details['file']['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($invoices_details['file']['updated_at']);
                    }
                }
            }
            if (!empty($case['quotes_details'])) {
                foreach ($case['quotes_details'] as &$quotes_details) {
                    $quotes_details['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($quotes_details['created_at']);
                    $quotes_details['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($quotes_details['updated_at']);
                    if (!empty($quotes_details['file'])) {
                        $quotes_details['file']['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($quotes_details['file']['created_at']);
                        $quotes_details['file']['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($quotes_details['file']['updated_at']);
                    }
                }
            }
            $case['case_full_details'] = [
                'apartments' => $case['apartment_details'],
                'area' => $case['area'],
                'assets' => $case['assets_details'],
                'completion_date' => $case['completion_date'],
                'contractors' => $case['contractor_details'],
                'created_at' => MybosTime::format_mongoDbTimeUsingBuildingTimezone($case['created_at'], $building->timezone, 'M d, Y'),
                'detail' => $case['detail'],
                'documents' => $case['documents'],
                'emails' => $case['emails'],
                'inventory_usages' => $case['inventory_usages'],
                'invoices' => $case['invoices'],
                'logs' => $case['logs'],
                'purchase_order_number' => $case['purchase_order_number'] ?? null,
                'number' => isset($case['number']) ? $case['number'] : null,
                'photos' => isset($case['photos']) ? $case['photos'] : [],
                'resized_photos' => isset($case['resized_photos']) ? $case['resized_photos'] : [],
                'start' => isset($case['start']) ? $case['start'] : null,
                'status' => isset($case['status']) ? $case['status'] : null,
                '_id' => isset($case['_id']) ? $case['_id'] : null,
                'priority' => isset($case['priority']) ? $case['priority'] : null,
                'type' => isset($case['type']) ? $case['type'] : null
            ];
        }

        // Return detailed-cases result.
        return $casesArray;
    }

    /**
     * NOTE: !!!IMPORTANT!!!  Do NOT use this function anymore for getting cases-list, unoptimized pagination... -Lino *
     * Todo: Delete this function later...
     */
    public static function list(CasesListFilter $filter, Building $building): array
    {
        $mongoDb = app('MongoDbClient');
        $casesCollection = $mongoDb->getCollection('cases');

        $mdbStage1Pipeline = [
            'deleted_at' => null,
            '$or' => [
                ['building_id' => $building->_id],
                [
                    /** Also get building synced cases **/
                    'duplicate_sync_building_ids' => [
                        '$in' => [$building->_id]
                    ]
                ]
            ]
        ];

        // Check case folder filter.
        if (!empty($filter->folder)) {
            $mdbStage1Pipeline['folders'] = [
                '$in' => [new ObjectId($filter->folder)]
            ];
        } else {
            // Check case status filter.
            if ($filter->status === MybosCaseSummaryItem::COMPLETED) {
                $caseStatusCompleted = $building->getCaseStatusBy('name', MybosBaseCaseStatus::COMPLETED);
                $filter->status = [$caseStatusCompleted->_id];
                $mdbStage1Pipeline['status_id'] = ['$in' => $filter->status, '$ne' => null];
            } else if ($filter->status === MybosCaseSummaryItem::CURRENT) {
                $caseStatusCompleted = $building->getCaseStatusBy('name', MybosBaseCaseStatus::COMPLETED);
                $caseStatusDeleted = $building->getCaseStatusBy('name', MybosBaseCaseStatus::DELETED);
                $filter->status = [$caseStatusCompleted->_id ?? '', $caseStatusDeleted->_id ?? ''];
                $mdbStage1Pipeline['status_id'] = ['$nin' => $filter->status, '$ne' => null];
            } else if ($filter->status === MybosCaseSummaryItem::TRASH) {
                $caseStatusDeleted = $building->getCaseStatusBy('name', MybosBaseCaseStatus::DELETED);
                $filter->status = [$caseStatusDeleted->_id];
                $mdbStage1Pipeline['status_id'] = ['$in' => $filter->status, '$ne' => null];
            } else if ($filter->status === MybosCaseSummaryItem::STARRED) {
                $mdbStage1Pipeline['starred'] = 1;
            } else if ($filter->status === MybosCaseSummaryItem::OVERDUE) {
                $caseStatusCompleted = $building->getCaseStatusBy('name', MybosBaseCaseStatus::COMPLETED);
                $caseStatusDeleted = $building->getCaseStatusBy('name', MybosBaseCaseStatus::DELETED);
                $carbonNow = MybosTime::now();
                $isoNow = MybosTime::dbCompatible($carbonNow);
                $mdbStage1Pipeline['due'] = ['$lt' => $isoNow];
                $mdbStage1Pipeline['status_id'] = ['$nin' => [$caseStatusCompleted->_id, $caseStatusDeleted->_id]];
            } else if ($filter->status === MybosCaseSummaryItem::DRAFT) {
                $mdbStage1Pipeline['status_id'] = ['$eq' => null];
            } else {
                // $filter->status === MybosCaseSummaryItem::CURRENT
                if (!empty($filter->status)) {
                    $caseStatus = Category::select('_id')->where('type', MybosCategoryType::CASE_STATUS)->get()->toArray();
                    $filter->status = array_column($caseStatus, '_id');
                } else {
                    $filter->status = [$filter->status];
                }
                $mdbStage1Pipeline['status_id'] = ['$in' => $filter->status, '$ne' => null];
            }
        }
        // Check sort details.
        $sortSpec = ['_id' => 1];       // Note: Can also be multiple sorted: ex: ['subject' => 1, 'number' => 1].
        if ($filter->sort_by != '') {

            // Set to priority sort number when sorting is by "case priority"
            if ($filter->sort_by == 'case_priority_name') {
                $filter->sort_by = 'case_priority_sort';
            }
            // Set to created_at_8601 when sorting is by "case draft" status
            if ($filter->sort_by == 'number' && $filter->status === MybosCaseSummaryItem::DRAFT) {
                $filter->sort_by = 'created_at_8601';
            }

            $sortSpec = [$filter->sort_by => 1];
            if ($filter->sort_mode == 'desc') {
                $sortSpec = [$filter->sort_by => -1];
            }
        }


        if (isset($filter->_ids) && !empty($filter->_ids)) {
            $objectID = [];
            foreach ($filter->_ids as $_id) {
                $objectID[] = new ObjectId($_id);
            }
            $mdbStage1Pipeline['_id'] = ['$in' => $objectID];
        }

        // Assemble query pipelines.
        $mdbPipeline = [
            ['$match' => $mdbStage1Pipeline]
        ];
        $mdbStage2Pipeline = [
            '$addFields' => [
                '_id_str' => [
                    // Get string converted _id.
                    '$toString' => '$_id'
                ],
                '_id_type_id' =>
                // Get ObjectId converted type_id.
                    MongoDbNative::_convertToNullOnError('$type_id', 'objectId'),
                '_id_priority_id' =>
                // Get ObjectId converted type_id.
                    MongoDbNative::_convertToNullOnError('$priority_id', 'objectId'),
                '_id_status_id' =>
                // Get ObjectId converted type_id.
                    MongoDbNative::_convertToNullOnError('$status_id', 'objectId'),
            ]
        ];

        // Case Types [Categories]
        $mdbStage3aPipeline = [
            '$lookup' => [
                'from' => 'categories',
                'localField' => '_id_type_id',
                'foreignField' => '_id',
                'pipeline' => [
                    [
                        '$project' => ['name' => 1, '_id' => 0]
                    ]
                ],
                'as' => 'case_type',
            ]
        ];
        $mdbStage3bPipeline = [
            '$unwind' => [
                'path' => '$case_type',
                'preserveNullAndEmptyArrays' => true
            ]
        ];
        $mdbStage3cPipeline = [
            '$addFields' => [
                'case_type_name' => '$case_type.name',
            ]
        ];

        // Case Priorities [Categories]
        $mdbStage3a2Pipeline = [
            '$lookup' => [
                'from' => 'categories',
                'localField' => '_id_priority_id',
                'foreignField' => '_id',
                'pipeline' => [
                    [
                        '$project' => ['name' => 1, 'sort' => 1, '_id' => 0]
                    ]
                ],
                'as' => 'case_priority',
            ]
        ];
        $mdbStage3b2Pipeline = [
            '$unwind' => [
                'path' => '$case_priority',
                'preserveNullAndEmptyArrays' => true
            ]
        ];
        $mdbStage3c2Pipeline = [
            '$addFields' => [
                'case_priority_sort' => '$case_priority.sort',
                'case_priority_name' => '$case_priority.name',
            ]
        ];

        // Contractors
        $mdbStage4aPipeline = [
            '$addFields' => [
                'contractor_w_objectIds' => [
                    '$map' => [
                        'input' => '$contractors',
                        'in' => [
                            '$mergeObjects' => [
                                '$$this',
                                [
                                    'contractor_id' => MongoDbNative::_convertToNullOnError('$$this.contractor_id', 'objectId'),
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        ];
        $mdbStage4bPipeline = [
            '$lookup' => [
                'from' => 'contractors',
                'localField' => 'contractor_w_objectIds.contractor_id',
                'foreignField' => '_id',
                'pipeline' => [
                    [
                        '$project' => [
                            'company_name' => 1, 'website' => 1, 'phone' => 1, 'email' => 1, 'contacts' => 1
                        ]
                    ]
                ],
                'as' => 'contractor_details',
            ]
        ];
        $mdbStage4cPipeline = [
            '$addFields' => [
                'contactor_company_names' => [
                    '$reduce' => [
                        'input' => '$contractor_details',
                        'initialValue' => '',
                        'in' => [
                            '$cond' => [
                                ['$eq' => ['$$value', '']],
                                ['$concat' => ['$$value', '$$this.company_name']],
                                ['$concat' => ['$$value', ', ', '$$this.company_name']]
                            ]
                        ]
                    ]
                ]
            ]
        ];

        // Apartments
        $mdbStage5aPipeline = [
            '$lookup' => [
                'from' => 'apartments',
                'localField' => 'apartments',
                'foreignField' => '_id',
                'pipeline' => [
                    [
                        '$project' => [
                            'unit_label' => 1, 'lot' => 1, 'status' => 1
                        ]
                    ]
                ],
                'as' => 'apartment_details',
            ]
        ];
        $mdbStage5bPipeline = [
            '$addFields' => [
                'apartment_unit_labels' => [
                    '$reduce' => [
                        'input' => '$apartment_details',
                        'initialValue' => '',
                        'in' => [
                            '$cond' => [
                                ['$eq' => ['$$value', '']],
                                ['$concat' => ['$$value', '$$this.unit_label']],
                                ['$concat' => ['$$value', ', ', '$$this.unit_label']]
                            ]
                        ]
                    ]
                ]
            ]
        ];

        // Status
        $mdbStage6aPipeline = [
            '$lookup' => [
                'from' => 'categories',
                'localField' => '_id_status_id',
                'foreignField' => '_id',
                'pipeline' => [
                    [
                        '$project' => ['name' => 1, '_id' => 0]
                    ]
                ],
                'as' => 'case_status',
            ]
        ];
        $mdbStage6bPipeline = [
            '$unwind' => [
                'path' => '$case_status',
                'preserveNullAndEmptyArrays' => true
            ]
        ];
        $mdbStage6cPipeline = [
            '$addFields' => [
                'case_status_name' => '$case_status.name',
            ]
        ];

        // Assets
        $mdbStage7aPipeline = [
            '$lookup' => [
                'from' => 'assets',
                'localField' => 'assets',
                'foreignField' => '_id',
                'pipeline' => [
                    [
                        '$project' => [
                            'name' => 1,
                        ]
                    ]
                ],
                'as' => 'assets_details',
            ]
        ];
        $mdbStage8aPipeline = [
            '$addFields' => [
                'job_area_name' => [
                    '$reduce' => [
                        'input' => '$area',
                        'initialValue' => '',
                        'in' => [
                            '$cond' => [
                                ['$eq' => ['$$value', '']],
                                ['$concat' => ['$$value', '$$this']],
                                ['$concat' => ['$$value', ', ', '$$this']]
                            ]
                        ]
                    ]
                ]
            ]
        ];
        $mdbStage9aPipeline = [
            '$lookup' => [
                'from' => 'assets',
                'localField' => 'assets',
                'foreignField' => '_id',
                'pipeline' => [
                    [
                        '$project' => [
                            'name' => 1, 'description' => 1, '_id' => ['$toString' => '$_id']
                        ]
                    ]
                ],
                'as' => 'assets_details',
            ]
        ];
        $mdbStage9bPipeline = [
            '$addFields' => [
                'asset_name' => [
                    '$reduce' => [
                        'input' => '$assets_details',
                        'initialValue' => '',
                        'in' => [
                            '$cond' => [
                                ['$eq' => ['$$value', '']],
                                ['$concat' => ['$$value', '$$this.name']],
                                ['$concat' => ['$$value', ', ', '$$this.name']]
                            ]
                        ]
                    ]
                ]
            ]
        ];

        $mdbStage10aPipeline = [
            '$addFields' => [
                'case_inventory_usage' => [
                    '$map' => [
                        'input' => '$inventory_usages',
                        'in' => [
                            '$mergeObjects' => [
                                '$$this',
                                [
                                    'inventory_id' => MongoDbNative::_convertToNullOnError('$$this.inventory_id', 'objectId'),
                                ]

                            ]
                        ]
                    ]
                ]
            ]
        ];
        $mdbStage11aPipeline = [
            // [Inventory Usages s2] : lookup inventory based on [inventory_usage_details] converted contractor_id above.
            '$lookup' => [
                'from' => 'inventories',
                'localField' => 'case_inventory_usage.inventory_id',
                'foreignField' => '_id',
                'pipeline' => [
                    [
                        '$project' => [
                            'item' => 1, 'location' => 1, 'description' => 1, 'owner' => 1, 'serial' => 1,
                            'value' => 1, 'sell' => 1, 'quantity' => 1, 'unlimited' => 1,
                        ]
                    ]
                ],
                'as' => 'inventory_details',
            ]
        ];
        $mdbStage12aPipeline = [
            '$addFields' => [
                'inventory_usages' => [
                    '$map' => [
                        'input' => '$case_inventory_usage',
                        'as' => 'ciu',
                        'in' => [
                            '$mergeObjects' => [
                                '$$ciu',
                                [
                                    'inventory' => [       // Add inventory details as embedded data. (Note: Alternatively, remove this to actually do a merge instead of embedding)
                                        '$first' => [
                                            '$filter' => [
                                                'input' => '$inventory_details',
                                                'cond' => [
                                                    '$eq' => ['$$this._id', '$$ciu.inventory_id']
                                                ]
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        ];
        $mdbStage13aPipeline = [
            // [Quotes s1] : convert contractor_id string to objectId
            '$addFields' => [
                'quotes_details' => [
                    '$map' => [
                        'input' => '$quotes',
                        'in' => [
                            '$mergeObjects' => [
                                '$$this',
                                [
                                    'contractor_id' => MongoDbNative::_convertToNullOnError('$$this.contractor_id', 'objectId'),
                                ]

                            ]
                        ]
                    ]
                ]
            ]
        ];
        $mdbStage14aPipeline = [
            // [Quotes s2] : lookup contractors based on [emails_details] converted contractor_id above.
            '$lookup' => [
                'from' => 'contractors',
                'localField' => 'quotes_details.contractor_id',
                'foreignField' => '_id',
                'pipeline' => [
                    [
                        '$project' => [
                            'company_name' => 1, 'website' => 1
                        ]
                    ]
                ],
                'as' => 'quotes_contractor_details',
            ]
        ];
        $mdbStage15aPipeline = [
            // [Quotes s3] : do $addFields here to filter out more fields to remove later below on the projection.
            '$addFields' => [
                'quotes' => [
                    '$map' => [
                        'input' => '$quotes_details',
                        'as' => 'ed',
                        'in' => [
                            '$mergeObjects' => [
                                '$$ed',
                                [
                                    'contractor' => [       // Add contractor details as embedded data.  (Note: Alternatively, remove this to actually do a merge instead of embedding)
                                        '$first' => [
                                            '$filter' => [
                                                'input' => '$quotes_contractor_details',
                                                'cond' => [
                                                    '$eq' => ['$$this._id', '$$ed.contractor_id']
                                                ]
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        ];
        $mdbStage16aPipeline = [
            // [Invoice s1] : convert contractor_id string to objectId
            '$addFields' => [
                'invoices_details' => [
                    '$map' => [
                        'input' => '$invoices',
                        'in' => [
                            '$mergeObjects' => [
                                '$$this',
                                [
                                    'contractor_id' => MongoDbNative::_convertToNullOnError('$$this.contractor_id', 'objectId'),
                                ]

                            ]
                        ]
                    ]
                ]
            ]
        ];
        $mdbStage17aPipeline = [
            // [Invoices s2] : lookup contractors based on [invoices_details] converted contractor_id above.
            '$lookup' => [
                'from' => 'contractors',
                'localField' => 'invoices_details.contractor_id',
                'foreignField' => '_id',
                'pipeline' => [
                    [
                        '$project' => [
                            'company_name' => 1, 'website' => 1
                        ]
                    ]
                ],
                'as' => 'invoices_contractor_details',
            ]
        ];
        $mdbStage18aPipeline = [
            // [Quotes s3] : do $addFields here to filter out more fields to remove later below on the projection.
            '$addFields' => [
                'invoices' => [
                    '$map' => [
                        'input' => '$invoices_details',
                        'as' => 'ed',
                        'in' => [
                            '$mergeObjects' => [
                                '$$ed',
                                [
                                    'contractor' => [       // Add contractor details as embedded data.  (Note: Alternatively, remove this to actually do a merge instead of embedding)
                                        '$first' => [
                                            '$filter' => [
                                                'input' => '$invoices_contractor_details',
                                                'cond' => [
                                                    '$eq' => ['$$this._id', '$$ed.contractor_id']
                                                ]
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        ];

        $mdbStage19aPipeline = [
            // [Status - Categories]
            '$lookup' => [
                'from' => 'categories',
                'let' => [
                    'statusId' => MongoDbNative::_convertToNullOnError('$status_id', 'objectId')
                ],
                'pipeline' => [
                    [
                        '$match' => [
                            '$expr' => [
                                '$eq' => ['$$statusId', '$_id']
                            ]
                        ]
                    ],
                    [
                        '$project' => [
                            'name' => 1, 'type' => 1
                        ]
                    ]
                ],
                'as' => 'status'
            ]
        ];
        $mdbStage20aPipeline = [
            // [Type - Categories]
            '$lookup' => [
                'from' => 'categories',
                'let' => [
                    'typeId' => MongoDbNative::_convertToNullOnError('$type_id', 'objectId')
                ],
                'pipeline' => [
                    [
                        '$match' => [
                            '$expr' => [
                                '$eq' => ['$$typeId', '$_id']
                            ]
                        ]
                    ],
                    [
                        '$project' => [
                            'name' => 1, 'type' => 1
                        ]
                    ]
                ],
                'as' => 'type'
            ]
        ];
        $mdbStage21aPipeline = [
            // [Priority - Categories]
            '$lookup' => [
                'from' => 'categories',
                'let' => [
                    'priorityId' => MongoDbNative::_convertToNullOnError('$priority_id', 'objectId')
                ],
                'pipeline' => [
                    [
                        '$match' => [
                            '$expr' => [
                                '$eq' => ['$$priorityId', '$_id']
                            ]
                        ]
                    ],
                    [
                        '$project' => [
                            'name' => 1, 'type' => 1
                        ]
                    ]
                ],
                'as' => 'priority'
            ]
        ];
        $mdbStage22aPipeline = [
            '$addFields' => [
                'start_8601' => MongoDbNative::_dateToString('$start', null, null),
                'due_8601' => MongoDbNative::_dateToString('$due', null, null),
                'created_at_8601' => MongoDbNative::_dateToString('$created_at', null, null),
            ]
        ];

        array_push(
            $mdbPipeline, $mdbStage2Pipeline,
            $mdbStage3aPipeline, $mdbStage3bPipeline, $mdbStage3cPipeline,
            $mdbStage3a2Pipeline, $mdbStage3b2Pipeline, $mdbStage3c2Pipeline,
            $mdbStage4aPipeline, $mdbStage4bPipeline, $mdbStage4cPipeline,
            $mdbStage5aPipeline, $mdbStage5bPipeline,
            $mdbStage6aPipeline, $mdbStage6bPipeline, $mdbStage6cPipeline,
            $mdbStage7aPipeline, $mdbStage8aPipeline, $mdbStage9aPipeline, $mdbStage9bPipeline, $mdbStage10aPipeline,
            $mdbStage11aPipeline, $mdbStage12aPipeline, $mdbStage13aPipeline, $mdbStage14aPipeline,
            $mdbStage15aPipeline, $mdbStage16aPipeline, $mdbStage17aPipeline, $mdbStage18aPipeline,
            $mdbStage19aPipeline, $mdbStage20aPipeline, $mdbStage21aPipeline, $mdbStage22aPipeline
        );

        // If search key is not empty.
        if (trim($filter->keyword) != '') {
            $mdbStageSearchPipeline = [];
            $filterKeyword = trim($filter->keyword);
            $mdbStageSearchPipeline['$match'] = [
                '$or' => [
                    ['area' => ['$regex' => $filterKeyword, '$options' => 'i']],
                    ['subject' => ['$regex' => $filterKeyword, '$options' => 'i']],
                    //['number' => (int)$filterKeyword],
                    ['$expr' => [
                        '$regexMatch' => [
                            'input' => ['$toString' => '$number'],
                            'regex' => $filterKeyword
                        ]
                    ]
                    ],
                    ['contactor_company_names' => ['$regex' => $filterKeyword, '$options' => 'i']],
                    ['case_type_name' => ['$regex' => $filterKeyword, '$options' => 'i']],
                    ['case_priority_name' => ['$regex' => $filterKeyword, '$options' => 'i']],
                    ['case_status_name' => ['$regex' => $filterKeyword, '$options' => 'i']],
                    ['priority' => ['$regex' => $filterKeyword, '$options' => 'i']],
                    ['apartment_unit_labels' => ['$regex' => $filterKeyword, '$options' => 'i']],
                    ['assets_name' => ['$regex' => $filterKeyword, '$options' => 'i']],
                ]
            ];
            array_push($mdbPipeline, $mdbStageSearchPipeline);
        }

        // Case Fields Filter. Initialize an empty pipeline.
        $mdbStageCaseFieldsFilterPipeline = [];

        // [due] filter.
        if (!empty($filter->due)) {
            $carbonNow = MybosTime::now();
            $isoEndOfThisWeek = MybosTime::dbCompatible($carbonNow->endOfWeek(CarbonInterface::SATURDAY));
            if ($filter->due == 'current_week') {
                $dueFilter = ['$lt' => $isoEndOfThisWeek];
            } else if ($filter->due == 'next_week') {
                $carbonNextWeek = MybosTime::now()->addWeeks(1);
                $isoEndOfNextWeek = MybosTime::dbCompatible($carbonNextWeek->endOfWeek(CarbonInterface::SATURDAY));
                $dueFilter = ['$gt' => $isoEndOfThisWeek, '$lt' => $isoEndOfNextWeek];
            } else {    // $filter->due == 'overdue'
                $isoNow = MybosTime::dbCompatible($carbonNow);
                $dueFilter = ['$lt' => $isoNow];
            }
            $mdbStageCaseFieldsFilterPipeline['due'] = $dueFilter;
        }

        // [priority] filter.
        if (!empty($filter->priority)) {
            $mdbStageCaseFieldsFilterPipeline['priority_id'] = $filter->priority;
        }

        // [contractors] filter.
        if (!empty($filter->contractors)) {
            $mdbStageCaseFieldsFilterPipeline['contractors.contractor_id'] = [
                '$in' => $filter->contractors
            ];
        }

        // [contractors] filter.
        if (!empty($filter->apartment)) {
            $apartmentObject = [];
            foreach ($filter->apartment as $apartment) {
                $apartmentObject[] = new ObjectId($apartment);
            }
            $mdbStageCaseFieldsFilterPipeline['apartments'] = [
                '$in' => $apartmentObject
            ];
        }

        // [contractors] filter.
        if (!empty($filter->contractors)) {
            $mdbStageCaseFieldsFilterPipeline['contractors.contractor_id'] = [
                '$in' => $filter->contractors
            ];
        }
        // [type] filter.
        if (!empty($filter->type)) {
            $mdbStageCaseFieldsFilterPipeline['type_id'] = ['$in' => $filter->type];
        }

        if (!empty($filter->asset_id)) {
            $mdbStageCaseFieldsFilterPipeline['assets'] = ['$in' => [new ObjectId($filter->asset_id)]];
        }

        // [days_open] filter.
        if (!empty($filter->days_open)) {
            $carbonNow = MybosTime::now();
            $isoNow = MybosTime::dbCompatible($carbonNow);
            $daysOpenFilter = [
                '$gt' => [
                    //[
                    //    '$dateDiff' => ['startDate' => '$start', 'endDate' => $isoNow, 'unit' => 'day']
                    //],
                    MongoDbNative::_safeDateDifference('$start', new UTCDateTime(), 'day', ''),
                    $filter->days_open
                ]
            ];
            $mdbStageCaseFieldsFilterPipeline['$expr'] = $daysOpenFilter;
        }

        if (!empty($filter->start_date) && !empty($filter->end_date)) {
            $start_date = MybosTime::CarbonToUTCDateTime(Carbon::createFromFormat('Y-m-d H:i:s', $filter->start_date . ' 00:00:00', $building['timezone']));
            $end_date = MybosTime::CarbonToUTCDateTime(Carbon::createFromFormat('Y-m-d H:i:s', $filter->end_date . ' 23:59:59', $building['timezone']));
            $mdbStageCaseFieldsFilterPipeline['$and'] = [
                [
                    'start' => ['$gte' => $start_date]
                ],
                [
                    'start' => ['$lte' => $end_date]
                ]
            ];
        }


        // Collect all case fields filter and add to pipeline as $match query.
        if (!empty($mdbStageCaseFieldsFilterPipeline)) {
            array_push($mdbPipeline, ['$match' => $mdbStageCaseFieldsFilterPipeline]);
        }

        //return ['data' => $mdbPipeline, 'filter' => $filter];

        $queryResult = $mongoDb->paginatedAggregationSearch(
            ['pageSize' => $filter->limit, 'pageNumber' => $filter->page, 'sort' => $sortSpec],
            $casesCollection, $mdbPipeline
        );

        $localeProfile = $building->locale_profile->toArray();

        // format dates.
        foreach ($queryResult['data'] as &$case) {
            $case['start'] = !empty($case['start']) ? MybosTime::format_mongoDbTimeUsingBuildingTimezone($case['start'], $localeProfile['timezone'], $localeProfile['date_format']) : '';
            $case['due'] = !empty($case['due']) ? MybosTime::format_mongoDbTimeUsingBuildingTimezone($case['due'], $localeProfile['timezone'], $localeProfile['date_format']) : '';
            $case['created'] = !empty($case['created_at']) ? MybosTime::format_mongoDbTimeUsingBuildingTimezone($case['created_at'], $localeProfile['timezone'], $localeProfile['date_format']) : '';
            if (!empty($case['photos'])) {
                foreach ($case['photos'] as &$photo) {
                    $photo['file']['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($photo['file']['created_at']);
                    $photo['file']['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($photo['file']['updated_at']);
                }
            }
            if (!empty($case['documents'])) {
                foreach ($case['documents'] as &$document) {
                    $document['file']['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($document['file']['created_at']);
                    $document['file']['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($document['file']['updated_at']);
                }
            }
            if (!empty($case['contractors'])) {
                foreach ($case['contractors'] as &$contractor) {
                    $contractor['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($contractor['created_at']);
                    $contractor['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($contractor['updated_at']);
                }
            }
            if (!empty($case['contractor_w_objectIds'])) {
                foreach ($case['contractor_w_objectIds'] as &$contractor_w_objectIds) {
                    $contractor_w_objectIds['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($contractor_w_objectIds['created_at']);
                    $contractor_w_objectIds['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($contractor_w_objectIds['updated_at']);
                }
            }
            if (!empty($case['emails'])) {
                foreach ($case['emails'] as &$email) {
                    $email['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($email['created_at']);
                    $email['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($email['updated_at']);
                    $email['due_by_8601'] = !empty($email['due_by']) ? MybosTime::UTCDateTimeToCarbon($email['due_by']) : null;
                }
            }
            if (!empty($case['logs'])) {
                foreach ($case['logs'] as &$log) {
                    $log['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($log['created_at']);
                    $log['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($log['updated_at']);
                }
            }
            if (!empty($case['quotes'])) {
                foreach ($case['quotes'] as &$quote) {
                    $quote['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($quote['created_at']);
                    $quote['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($quote['updated_at']);
                    if (!empty($quote['file'])) {
                        $quote['file']['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($quote['file']['created_at']);
                        $quote['file']['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($quote['file']['updated_at']);
                    }
                }
            }
            if (!empty($case['inventory_usages'])) {
                foreach ($case['inventory_usages'] as &$inventory_usage) {
                    $inventory_usage['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($inventory_usage['created_at']);
                    $inventory_usage['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($inventory_usage['updated_at']);
                }
            }
            if (!empty($case['resized_photos'])) {
                foreach ($case['resized_photos'] as &$resized_photos) {
                    $resized_photos['aws_s3']['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($resized_photos['aws_s3']['created_at']);
                    $resized_photos['aws_s3']['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($resized_photos['aws_s3']['updated_at']);
                }
            }
            if (!empty($case['invoices'])) {
                foreach ($case['invoices'] as &$invoice) {
                    $invoice['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($invoice['created_at']);
                    $invoice['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($invoice['updated_at']);
                    if (!empty($invoice['file'])) {
                        $invoice['file']['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($invoice['file']['created_at']);
                        $invoice['file']['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($invoice['file']['updated_at']);
                    }
                }
            }
            if (!empty($case['invoices_details'])) {
                foreach ($case['invoices_details'] as &$invoices_details) {
                    $invoices_details['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($invoices_details['created_at']);
                    $invoices_details['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($invoices_details['updated_at']);
                    if (!empty($invoices_details['file'])) {
                        $invoices_details['file']['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($invoices_details['file']['created_at']);
                        $invoices_details['file']['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($invoices_details['file']['updated_at']);
                    }
                }
            }
            if (!empty($case['quotes_details'])) {
                foreach ($case['quotes_details'] as &$quotes_details) {
                    $quotes_details['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($quotes_details['created_at']);
                    $quotes_details['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($quotes_details['updated_at']);
                    if (!empty($quotes_details['file'])) {
                        $quotes_details['file']['created_at_8601'] = MybosTime::UTCDateTimeToCarbon($quotes_details['file']['created_at']);
                        $quotes_details['file']['updated_at_8601'] = MybosTime::UTCDateTimeToCarbon($quotes_details['file']['updated_at']);
                    }
                }
            }
            $case['case_full_details'] = [
                'apartments' => $case['apartment_details'],
                'area' => $case['area'],
                'assets' => $case['assets_details'],
                'completion_date' => $case['completion_date'],
                'contractors' => $case['contractor_details'],
                'created_at' => MybosTime::format_mongoDbTimeUsingBuildingTimezone($case['created_at'], $building->timezone, 'M d, Y'),
                'detail' => $case['detail'],
                'documents' => $case['documents'],
                'emails' => $case['emails'],
                'inventory_usages' => $case['inventory_usages'],
                'invoices' => $case['invoices'],
                'logs' => $case['logs'],
                'purchase_order_number' => $case['purchase_order_number'] ?? null,
                'number' => isset($case['number']) ? $case['number'] : null,
                'photos' => isset($case['photos']) ? $case['photos'] : [],
                'resized_photos' => isset($case['resized_photos']) ? $case['resized_photos'] : [],
                'start' => isset($case['start']) ? $case['start'] : null,
                'status' => isset($case['status']) ? $case['status'] : null,
                '_id' => isset($case['_id']) ? $case['_id'] : null,
                'priority' => isset($case['priority']) ? $case['priority'] : null,
                'type' => isset($case['type']) ? $case['type'] : null
            ];
        }

        return [
            'data' => $queryResult,
            'filter' => $filter,
        ];
    }


    /**
     * @param $filter
     * @param $status
     * @param $statusLoadMore
     * @param Building $building
     * @return array
     */
    public static function getPipeline($filter, $status, $statusLoadMore, Building $building): array
    {
        $timezone = MybosUserSession::getBestTimezone();

        // Set case status filter.
        $mdbStage1Pipeline['status_id'] = ['$eq' => (string)$status->_id];

        // Check case folder filter.
        if (!empty($filter->folder)) {
            $mdbStage1Pipeline['folders'] = [
                '$in' => [new ObjectId($filter->folder)]
            ];
        } else {
            if ($filter->status === MybosCaseSummaryItem::STARRED) {
                $mdbStage1Pipeline['starred'] = 1;
            } else if ($filter->status === MybosCaseSummaryItem::OVERDUE) {
                $carbonNow = MybosTime::now();
                $isoNow = MybosTime::dbCompatible($carbonNow);
                $mdbStage1Pipeline['due'] = ['$lt' => $isoNow];
            }
        }

        // Check sort details.
        $sortSpec = ['_id' => 1];       // Note: Can also be multiple sorted: eCx: ['subject' => 1, 'number' => 1].
        if ($filter->sort_by != '') {
            $sortSpec = [$filter->sort_by => 1];
            if ($filter->sort_mode == 'desc') {
                $sortSpec = [$filter->sort_by => -1];
            }
        }

        // Assemble query pipelines.
        $mdbPipeline = [
            ['$match' => $mdbStage1Pipeline],
            ['$sort' => $sortSpec]
        ];
        $mdbStage2Pipeline = [
            '$addFields' => [
                '_id_str' => [
                    // Get string converted _id.
                    '$toString' => '$_id'
                ],
                '_id_type_id' => [
                    // Get ObjectId converted type_id.
                    '$toObjectId' => '$type_id'
                ],
                '_id_status_id' => [
                    // Get ObjectId converted type_id.
                    '$toObjectId' => '$status_id'
                ],
                '_id_priority_id' => [
                    // Get ObjectId converted type_id.
                    '$toObjectId' => '$priority_id'
                ],
                'start' => [
                    '$toString' => '$start'
                ],
                'due_8601' => MongoDbNative::_dateToString('$due'),
                'due' => MongoDbNative::_dateToString('$due', '%d/%m/%Y', $timezone),
            ]
        ];

        // Case Types [Categories]
        $mdbStage3aPipeline = [
            '$lookup' => [
                'from' => 'categories',
                'localField' => '_id_type_id',
                'foreignField' => '_id',
                'pipeline' => [
                    [
                        '$project' => ['name' => 1, '_id' => 0]
                    ]
                ],
                'as' => 'case_type',
            ]
        ];
        $mdbStage3bPipeline = [
            '$unwind' => [
                'path' => '$case_type',
                'preserveNullAndEmptyArrays' => true
            ]
        ];
        $mdbStage3cPipeline = [
            '$addFields' => [
                'case_type_name' => '$case_type.name',
            ]
        ];

        // Contractors
        $mdbStage4aPipeline = [
            '$addFields' => [
                'contractor_w_objectIds' => [
                    '$map' => [
                        'input' => '$contractors',
                        'in' => [
                            '$mergeObjects' => [
                                '$$this',
                                [
                                    'contractor_id' => ['$toObjectId' => '$$this.contractor_id']
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        ];
        $mdbStage4bPipeline = [
            '$lookup' => [
                'from' => 'contractors',
                'localField' => 'contractor_w_objectIds.contractor_id',
                'foreignField' => '_id',
                'pipeline' => [
                    [
                        '$project' => [
                            'company_name' => 1, 'website' => 1, 'phone' => 1
                        ]
                    ]
                ],
                'as' => 'contractor_details',
            ]
        ];
        $mdbStage4cPipeline = [
            '$addFields' => [
                'contactor_company_names' => [
                    '$reduce' => [
                        'input' => '$contractor_details',
                        'initialValue' => '',
                        'in' => [
                            '$cond' => [
                                ['$eq' => ['$$value', '']],
                                ['$concat' => ['$$value', '$$this.company_name']],
                                ['$concat' => ['$$value', ', ', '$$this.company_name']]
                            ]
                        ]
                    ]
                ]
            ]
        ];

        // Apartments
        $mdbStage5aPipeline = [
            '$lookup' => [
                'from' => 'apartments',
                'localField' => 'apartments',
                'foreignField' => '_id',
                'pipeline' => [
                    [
                        '$project' => [
                            'unit_label' => 1, 'lot' => 1, 'status' => 1
                        ]
                    ]
                ],
                'as' => 'apartment_details',
            ]
        ];
        $mdbStage5bPipeline = [
            '$addFields' => [
                'apartment_unit_labels' => [
                    '$reduce' => [
                        'input' => '$apartment_details',
                        'initialValue' => '',
                        'in' => [
                            '$cond' => [
                                ['$eq' => ['$$value', '']],
                                ['$concat' => ['$$value', '$$this.unit_label']],
                                ['$concat' => ['$$value', ', ', '$$this.unit_label']]
                            ]
                        ]
                    ]
                ]
            ]
        ];

        // Status
        $mdbStage6aPipeline = [
            '$lookup' => [
                'from' => 'categories',
                'localField' => '_id_status_id',
                'foreignField' => '_id',
                'pipeline' => [
                    [
                        '$project' => ['name' => 1, '_id' => 0]
                    ]
                ],
                'as' => 'case_status',
            ]
        ];
        $mdbStage6bPipeline = [
            '$unwind' => [
                'path' => '$case_status',
                'preserveNullAndEmptyArrays' => true
            ]
        ];
        $mdbStage6cPipeline = [
            '$addFields' => [
                'case_status_name' => '$case_status.name',
            ]
        ];

        // Assets
        $mdbStage7aPipeline = [
            '$lookup' => [
                'from' => 'assets',
                'localField' => 'assets',
                'foreignField' => '_id',
                'pipeline' => [
                    [
                        '$project' => [
                            'name' => 1,
                        ]
                    ]
                ],
                'as' => 'assets_details',
            ]
        ];
        $mdbStage7bPipeline = [
            '$addFields' => [
                'assets_name' => [
                    '$reduce' => [
                        'input' => '$assets_details',
                        'initialValue' => '',
                        'in' => [
                            '$cond' => [
                                ['$eq' => ['$$value', '']],
                                ['$concat' => ['$$value', '$$this.name']],
                                ['$concat' => ['$$value', ', ', '$$this.name']]
                            ]
                        ]
                    ]
                ]
            ]
        ];

        // Priority
        $mdbStage8aPipeline = [
            '$lookup' => [
                'from' => 'categories',
                'localField' => '_id_priority_id',
                'foreignField' => '_id',
                'pipeline' => [
                    [
                        '$project' => ['name' => 1, '_id' => 0]
                    ]
                ],
                'as' => 'case_priority',
            ]
        ];
        $mdbStage8bPipeline = [
            '$unwind' => [
                'path' => '$case_priority',
                'preserveNullAndEmptyArrays' => true
            ]
        ];
        $mdbStage8cPipeline = [
            '$addFields' => [
                'case_priority_name' => '$case_priority.name',
            ]
        ];

        array_push(
            $mdbPipeline, $mdbStage2Pipeline,
            $mdbStage3aPipeline, $mdbStage3bPipeline, $mdbStage3cPipeline,
            $mdbStage4aPipeline, $mdbStage4bPipeline, $mdbStage4cPipeline,
            $mdbStage5aPipeline, $mdbStage5bPipeline,
            $mdbStage6aPipeline, $mdbStage6bPipeline, $mdbStage6cPipeline,
            $mdbStage7aPipeline, $mdbStage7bPipeline,
            $mdbStage8aPipeline, $mdbStage8bPipeline, $mdbStage8cPipeline,
        );

        // If search key is not empty.
        if (trim($filter->keyword) != '') {
            $mdbStageSearchPipeline = [];
            $filterKeyword = trim($filter->keyword);
            $mdbStageSearchPipeline['$match'] = [
                '$or' => [
                    ['area' => ['$regex' => $filterKeyword, '$options' => 'i']],
                    ['subject' => ['$regex' => $filterKeyword, '$options' => 'i']],
                    //['number' => (int)$filterKeyword],
                    ['$expr' => [
                        '$regexMatch' => [
                            'input' => ['$toString' => '$number'],
                            'regex' => $filterKeyword
                        ]
                    ]
                    ],
                    ['contactor_company_names' => ['$regex' => $filterKeyword, '$options' => 'i']],
                    ['case_type_name' => ['$regex' => $filterKeyword, '$options' => 'i']],
                    ['case_status_name' => ['$regex' => $filterKeyword, '$options' => 'i']],
                    ['case_priority_name' => ['$regex' => $filterKeyword, '$options' => 'i']],
                    ['apartment_unit_labels' => ['$regex' => $filterKeyword, '$options' => 'i']],
                    ['assets_name' => ['$regex' => $filterKeyword, '$options' => 'i']],
                ]
            ];
            array_push($mdbPipeline, $mdbStageSearchPipeline);
        }

        // Case Fields Filter. Initialize an empty pipeline.
        $mdbStageCaseFieldsFilterPipeline = [];

        // [due] filter.
        if (!empty($filter->due)) {
            $carbonNow = MybosTime::now();
            $isoEndOfThisWeek = MybosTime::dbCompatible($carbonNow->endOfWeek(CarbonInterface::SATURDAY));
            if ($filter->due == 'current_week') {
                $dueFilter = ['$lt' => $isoEndOfThisWeek];
            } else if ($filter->due == 'next_week') {
                $carbonNextWeek = MybosTime::now()->addWeeks(1);
                $isoEndOfNextWeek = MybosTime::dbCompatible($carbonNextWeek->endOfWeek(CarbonInterface::SATURDAY));
                $dueFilter = ['$gt' => $isoEndOfThisWeek, '$lt' => $isoEndOfNextWeek];
            } else {    // $filter->due == 'overdue'
                $isoNow = MybosTime::dbCompatible($carbonNow);
                $dueFilter = ['$lt' => $isoNow];
            }
            $mdbStageCaseFieldsFilterPipeline['due'] = $dueFilter;
        }

        // [priority] filter.
        if (!empty($filter->priority)) {
            $mdbStageCaseFieldsFilterPipeline['priority_id'] = $filter->priority;
        }

        // [contractors] filter.
        if (!empty($filter->contractors)) {
            $mdbStageCaseFieldsFilterPipeline['contractors.contractor_id'] = [
                '$in' => $filter->contractors
            ];
        }

        // [type] filter.
        if (!empty($filter->type)) {
            $mdbStageCaseFieldsFilterPipeline['type_id'] = ['$in' => $filter->type];
        }

        // [days_open] filter.
        if (!empty($filter->days_open)) {
            $daysOpenFilter = [
                '$gt' => [
                    //[
                    //    '$dateDiff' => ['startDate' => '$start', 'endDate' => $isoNow, 'unit' => 'day']
                    //],
                    MongoDbNative::_safeDateDifference('$start', new UTCDateTime(), 'day', ''),
                    $filter->days_open
                ]
            ];
            $mdbStageCaseFieldsFilterPipeline['$expr'] = $daysOpenFilter;
        }

        // Collect all case fields filter and add to pipeline as $match query.
        if (!empty($mdbStageCaseFieldsFilterPipeline)) {
            array_push($mdbPipeline, ['$match' => $mdbStageCaseFieldsFilterPipeline]);
        }
        $page = 1;

        if (isset($statusLoadMore['status']) && $statusLoadMore['status'] == $status->name) {
            $page = $statusLoadMore['page'];
        }

        array_push($mdbPipeline, ['$skip' => ($page - 1) * 20]);
        array_push($mdbPipeline, ['$limit' => 21]);

        return $mdbPipeline;
    }


    /**
     * @param CasesListFilter $filter
     * @param Building $building
     * @param $statusLoadMore
     * @return array
     */
    public static function GetAllByStatus(CasesListFilter $filter, Building $building, $statusLoadMore = null): array
    {
        $mongoDb = app('MongoDbClient');

        /** Get all building case-status categories. **/
        $categoryCollection = $mongoDb->getCollection('categories');
        $mdbStage1Pipeline['deleted_at'] = null;
        $mdbStage1Pipeline['building_id'] = ['$eq' => $building['_id']];
        $mdbStage1Pipeline['type'] = ['$eq' => MybosCategoryType::CASE_STATUS];
        $status_list = $categoryCollection->aggregate(
            [
                [
                    '$match' => $mdbStage1Pipeline
                ],
                [
                    '$sort' => [
                        'sort' => 1
                    ]
                ]
            ]
        );
        $status_list = $status_list->toArray();
        $facetData = [];
        foreach ($status_list as $status) {
            $facetData[$status['name']] = self::getPipeline($filter, $status, $statusLoadMore, $building);
            $caseArray[$status->name] = [];
        }

        /** Assemble $match initial pipeline stage. **/
        $mdbInitialMatchPipelineStage = [
            'building_id' => $building['_id'],
            'deleted_at' => null,
        ];

        /** Get all cases faceted via all building's case-status. **/
        $casesCollection = $mongoDb->getCollection('cases');
        $queryResult = $casesCollection->aggregate([
            [
                '$match' => $mdbInitialMatchPipelineStage
            ],
            [
                '$facet' => $facetData,
            ]
        ]);

        $queryResult = $queryResult->toArray();
        $caseArray = [];
        if ($filter->status === MybosCaseSummaryItem::COMPLETED) {
            $caseArray[MybosBaseCaseStatus::COMPLETED] = $queryResult[0][MybosBaseCaseStatus::COMPLETED];
        } else if ($filter->status === MybosCaseSummaryItem::TRASH) {
            $caseArray[MybosBaseCaseStatus::DELETED] = $queryResult[0][MybosBaseCaseStatus::DELETED];
        } else if ($filter->status == 'Current') {
            $caseStatusCompleted = $building->getCaseStatusBy('name', MybosBaseCaseStatus::COMPLETED);
            $caseStatusDeleted = $building->getCaseStatusBy('name', MybosBaseCaseStatus::DELETED);
            $caseStatus = Category::select('name')->where('type', MybosCategoryType::CASE_STATUS)->where('building_id', $building['_id'])->whereNotIn('_id', [$caseStatusCompleted->_id, $caseStatusDeleted->_id])->get()->toArray();
            foreach ($caseStatus as $status) {
                $caseArray[$status['name']] = $queryResult[0][$status['name']];
            }
        } else if ($filter->status == 'Overdue') {
            $caseStatusCompleted = $building->getCaseStatusBy('name', MybosBaseCaseStatus::COMPLETED);
            $caseStatus = Category::select('name')->where('type', MybosCategoryType::CASE_STATUS)->where('building_id', $building['_id'])->where('_id', '!=', $caseStatusCompleted->_id)->get()->toArray();
            foreach ($caseStatus as $status) {
                $caseArray[$status['name']] = $queryResult[0][$status['name']];
            }
        } else {
            foreach ($queryResult[0] as $key => $data) {
                $caseArray[$key] = $data;
            }
        }

        $rv = [];
        foreach ($status_list as $status) {
            if (isset($caseArray[$status['name']])) {
                $current_page = 1;
                if (isset($statusLoadMore['status']) && $statusLoadMore['status'] == $status->name) {
                    $current_page = $statusLoadMore['page'];
                }

                $loadMore = false;
                if (isset($caseArray[$status['name']]) && count($caseArray[$status['name']]) > 20) {
                    unset($caseArray[$status['name']][count($caseArray[$status['name']]) - 1]);
                    $loadMore = true;
                }


                $rv[] = [
                    '_id' => $status->_id,
                    'name' => $status['name'],
                    'cases' => [
                        'current_page' => $current_page,
                        'data' => $caseArray[$status['name']],
                        'load_more' => $loadMore
                    ]
                ];
            }
        }
        return $rv;
    }

    /**
     * @param User $user
     * @param Building $building
     * @param $case
     * @return string
     */
    public static function exportPdf(User $user, Building $building, $case): string
    {
        // Get User Profile(email, mobile and signature)
        $mailProfile = $user->findMailProfile($building['_id'], true);
        $case['signature'] = $mailProfile['signature'] ?? '';
        $case['contactMobile'] = $mailProfile['mobile'] ?? '';
        $case['contactEmail'] = $mailProfile['email'] ?? '';

        $case['session_user'] = $user;
        $case['building_details'] = $building;
        $s3Bucket = new AwsS3();
        if (!empty($case['photos']) && count($case['photos']) > 0) {
            foreach ($case['photos'] as &$photo) {
                $secure_url = $s3Bucket->preSecureDocumentUrl($photo['file']['s3key']);
                $fileName = explode('.', $photo['file']['file_name']);
                $extension = $fileName[1] ?? 'png';
                $image = "data:image/".$extension.";base64,".base64_encode(file_get_contents($secure_url));
                $photo['file_path'] = $image;
            }
        }
        if (!empty($case['documents']) && count($case['documents']) > 0) {
            foreach ($case['documents'] as &$document) {
                $s3Bucket->appendSecureUrl($document['file']);
            }
        }
        if (!empty($case['quotes']) && count($case['quotes']) > 0) {
            foreach ($case['quotes'] as &$quote) {
                $s3Bucket->appendSecureUrl($quote['file']);
            }
        }
        if (!empty($case['invoices']) && count($case['invoices']) > 0) {
            foreach ($case['invoices'] as &$invoice) {
                $s3Bucket->appendSecureUrl($invoice['file']);
            }
        }

        $case['currencySymbol'] = Currency::find($building->locale->selected_country_profile->currency_code)->symbol;
        /** Set work-order-logo image. (fallback to company-logo, else fallback to mybos image at img/mybos-logo.png) **/
        $buildingWorkOrderLogo = $building->getWorkOrderLogo(true);     // Get work-order-logo with fallback.
        if (!empty($buildingWorkOrderLogo)) {
            $secure_url = $s3Bucket->preSecureDocumentUrl($buildingWorkOrderLogo['s3key']);
            $fileName = explode('.', $buildingWorkOrderLogo['file_name']);
            $extension = $fileName[1] ?? 'png';
            $filePathWorkOrderLogo = "data:image/" . $extension . ";base64," . base64_encode(file_get_contents($secure_url));
        } else {
            $filePathWorkOrderLogo = public_path('img/mybos-logo.png');       // Final fallback, use mybos logo...
        }
        $case['work_order_logo'] = $filePathWorkOrderLogo;
        $pdf = PDF::loadView('PDF.case-view', ['case' => $case]);
        return $pdf->output();
    }


    /**
     * @param Building $building
     * @param User $user
     * @param Cases $case
     * @throws Exception
     */
    public static function exportDoc(Building $building, User $user, Cases $case): void
    {
        $logo_path = public_path('img/mybos-logo.png');
        $s3Bucket = new AwsS3();
        $imagePath = [];
        Storage::disk('public')->makeDirectory('temp');

        /** Set work-order-logo image. (fallback to company-logo, else fallback to mybos image at img/mybos-logo.png) **/
        $buildingWorkOrderLogo = $building->getWorkOrderLogo(true);     // Get work-order-logo with fallback.
        if (!empty($buildingWorkOrderLogo)) {
            $secure_url = $s3Bucket->preSecureDocumentUrl($buildingWorkOrderLogo['s3key']);
            $fileContent = file_get_contents($secure_url);
        } else {
            $filePathMybosLogo = public_path('img/mybos-logo.png');       // Final fallback, use mybos logo...
            $fileContent = file_get_contents($filePathMybosLogo);
        }
        $logo_path = MybosFile::createFile('imageFile', $fileContent);

        $currencySymbol = Currency::find($building->locale->selected_country_profile->currency_code)->symbol;

        $case = self::getCaseFullDetails($case->_id);
        $localeProfile = $building->locale_profile->toArray();
        $themeColor = '00B0F0';
        $dangerColor = 'ff4757';
        $linkColor = '5352ed';

        $document = new \App\Services\PhpWordDocument();
        $document->getObject()->setDefaultFontName('Calibri');
        $document->getObject()->setDefaultFontSize(11);
        $document->properties()->setTitle('Building Management Report');

        // Start Content Section
        $section = $document->addSection();
        $section->getStyle()->setPageNumberingStart(1);

        // Create Top Layout Table
        $headerTable = $section->addTable(['width' => 100, 'borderSize' => 5, 'borderColor' => 'ffffff']);
        // Add a Row
        $headerTable->addRow();
        // Add Two cell inside the row
        $logoCell = $headerTable->addCell(ReportRepository::twip(9), ['borderColor' => 'ffffff', 'borderSize' => 5, 'valign' => 'center']);
        $infoCell = $headerTable->addCell(
            ReportRepository::twip(9),
            ['borderColor' => 'ffffff', 'borderSize' => 6]
        );
        // Add Logo in Logo Cell
        $logoCell->addImage(
            $logo_path,
            ['width' => ReportRepository::point(5), 'alignment' => 'start', 'marginLeft' => 0]
        );
        // Create New table inside Info Cell
        $headerTableRightCellTable = $infoCell->addTable(['width' => 100, 'borderSize' => 5, 'borderColor' => 'ffffff', 'cellMargin' => 50, 'cellSpacing' => 15]);
        // Add a new Row (Case #)
        $headerTableRightCellTable->addRow(150);
        $caseNoCell = $headerTableRightCellTable->addCell(ReportRepository::twip(5), ['bgColor' => $themeColor, 'borderColor' => $themeColor, 'borderSize' => 10, 'cellSpacing' => 20]);
        $caseNoCell->addText('  CASE #', ['bold' => true, 'color' => 'ffffff'], ['spaceAfter' => 2]);
        $caseNoValueCell = $headerTableRightCellTable->addCell(ReportRepository::twip(3), ['bgColor' => 'ffffff', 'borderColor' => '5b5858', 'borderSize' => 10]);
        $caseNoValueCell->addText($case->number, ['bold' => false, 'color' => '444444'], ['spaceAfter' => 2, 'align' => 'center']);
        // Add a new Row (DATE CREATED)
        $headerTableRightCellTable->addRow(150);
        $caseCreatedCell = $headerTableRightCellTable->addCell(ReportRepository::twip(5), ['bgColor' => $themeColor, 'borderColor' => $themeColor, 'borderSize' => 10, 'cellSpacing' => 20]);
        $caseCreatedCell->addText('  DATE CREATED', ['bold' => true, 'color' => 'ffffff'], ['spaceAfter' => 2]);
        $caseCreatedValCell = $headerTableRightCellTable->addCell(ReportRepository::twip(3), ['bgColor' => 'ffffff', 'borderColor' => '5b5858', 'borderSize' => 10]);
        $caseCreatedValCell->addText(MybosTime::format_mongoDbTimeUsingBuildingTimezone($case['created_at'], $localeProfile['timezone'], $localeProfile['date_format']), ['bold' => false, 'color' => '444444'], ['spaceAfter' => 2, 'align' => 'center']);
        // Add a new Row (CASE DUE BY)
        $headerTableRightCellTable->addRow(150);
        $caseDueCell = $headerTableRightCellTable->addCell(ReportRepository::twip(5), ['bgColor' => $themeColor, 'borderColor' => $themeColor, 'borderSize' => 10, 'cellSpacing' => 20]);
        $caseDueCell->addText('  CASE DUE BY', ['bold' => true, 'color' => 'ffffff'], ['spaceAfter' => 2]);
        $caseDueValCell = $headerTableRightCellTable->addCell(ReportRepository::twip(3), ['bgColor' => 'ffffff', 'borderColor' => '5b5858', 'borderSize' => 10]);
        $caseDueValCell->addText(MybosTime::format_mongoDbTimeUsingBuildingTimezone($case['due'],  $localeProfile['timezone'], $localeProfile['date_format']), ['bold' => false, 'color' => '444444'], ['spaceAfter' => 2, 'align' => 'center']);
        // Add a new Row (CASE PRIORITY)
        $headerTableRightCellTable->addRow(150);
        $casePriorityCell = $headerTableRightCellTable->addCell(ReportRepository::twip(5), ['bgColor' => $themeColor, 'borderColor' => $themeColor, 'borderSize' => 10, 'cellSpacing' => 20]);
        $casePriorityCell->addText('  PRIORITY', ['bold' => true, 'color' => 'ffffff'], ['spaceAfter' => 2]);
        $casePriorityValCell = $headerTableRightCellTable->addCell(ReportRepository::twip(3), ['bgColor' => 'ffffff', 'borderColor' => '5b5858', 'borderSize' => 10]);
        $casePriorityValCell->addText($case->priority['name'] ?? '-', ['bold' => false, 'color' => '444444'], ['spaceAfter' => 2, 'align' => 'center']);

        // Add a new Row (Purchase Order Number)
        $headerTableRightCellTable->addRow(150);
        $casePriorityCell = $headerTableRightCellTable->addCell(ReportRepository::twip(5), ['bgColor' => $themeColor, 'borderColor' => $themeColor, 'borderSize' => 10, 'cellSpacing' => 20]);
        $casePriorityCell->addText('  PO NUMBER', ['bold' => true, 'color' => 'ffffff'], ['spaceAfter' => 2]);
        $casePriorityValCell = $headerTableRightCellTable->addCell(ReportRepository::twip(3), ['bgColor' => 'ffffff', 'borderColor' => '5b5858', 'borderSize' => 10]);
        $casePriorityValCell->addText($case->purchase_order_number ?? '-', ['bold' => false, 'color' => '444444'], ['spaceAfter' => 2, 'align' => 'center']);


        // Create Building Contact Layout Table
        $buildingContactTable = $section->addTable(['width' => 100, 'borderSize' => 5, 'borderColor' => 'ffffff']);
        // Add a Row
        $buildingContactTable->addRow();
        $buildingInfoCell = $buildingContactTable->addCell(ReportRepository::twip(18), ['borderColor' => 'ffffff', 'borderSize' => 5]);
        $buildingInfoCell->addText(MybosCaseHelper::getCompanyDisplayNameWithFallbacks($building), ['color' => $themeColor, 'bold' => true], ['space' => array('before' => 0, 'after' => 0)]);
        $buildingInfoCell->addText($building['name'] . ' - ' . $building['plan'], ['color' => '444444'], ['space' => array('before' => 0, 'after' => 0)]);
        $buildingInfoCell->addText($building['address']['street_address'] .' '.$building['address']['suburb'].' '.$building['address']['state'] .' '.$building['address']['post_code'], ['color' => '444444'], ['space' => array('before' => 0, 'after' => 0)]);
        $buildingInfoCell->addText(date('M d, Y'), ['color' => '444444'], ['space' => array('before' => 0, 'after' => 0)]);
        // Add a Row
        $buildingInfoCell->addText('', ['color' => '444444'], ['space' => array('before' => 0, 'after' => 0)]);
        $buildingContactTable->addRow();
        $contactInfoCell = $buildingContactTable->addCell(ReportRepository::twip(18), ['borderColor' => 'ffffff', 'borderSize' => 5]);
        $contactInfoCell->addText('Site Contact', ['color' => $themeColor, 'bold' => true], ['space' => array('before' => 0, 'after' => 0)]);
        $contactInfoCell->addText($user['first_name'] . ' ' . $user['last_name'], ['color' => '444444'], ['space' => array('before' => 0, 'after' => 0)]);

        // Add row for contact mobile and email.
        $mailProfile = $user->findMailProfile($building['_id'], true);
        $contactMobile = $mailProfile['mobile'] ?? '';
        $contactEmail = $mailProfile['email'] ?? '';
        $contactInfoCell->addText($contactMobile, ['color' => '444444'], ['space' => array('before' => 0, 'after' => 0)]);
        $contactInfoCell->addText($contactEmail, ['color' => '444444'], ['space' => array('before' => 0, 'after' => 0)]);
        $contactInfoCell->addText('', ['color' => '444444'], ['space' => array('before' => 0, 'after' => 0)]);

        // Create Assigned To Layout Table
        $assignedToTable = $section->addTable(['width' => 100, 'borderSize' => 5, 'borderColor' => 'ffffff']);
        $assignedToTable->addRow();
        $assignedToHeaderCell = $assignedToTable->addCell(ReportRepository::twip(18), ['bgColor' => $themeColor, 'valign' => 'center']);
        $assignedToHeaderCell->addText('  ASSIGNED TO', ['bold' => true, 'color' => 'ffffff', 'size' => 14], ['space' => array('before' => 100, 'after' => 100)]);
        $assignedToTable->addRow();
        $assignedToInfoCell = $assignedToTable->addCell(ReportRepository::twip(18), ['valign' => 'center']);
        $assignedToInfoCell->addText('', ['bold' => false, 'color' => '444444'], ['space' => array('before' => 0, 'after' => 0)]);
        foreach ($case->contractors as $contractor) {
            $contractorObj = Contractor::find($contractor['contractor_id']);
            $contactNumber = !empty( $contractorObj['contacts'][0]['number'] ) ?  $contractorObj['contacts'][0]['number'] : '';
            $assignedToInfoCell->addText($contractorObj->company_name . ' - MOB: ' . $contactNumber. ' - PH: ' . $contractorObj->phone ?? '', ['bold' => false, 'color' => '444444'], ['space' => array('before' => 0, 'after' => 0)]);
        }
        $assignedToInfoCell->addText('', ['bold' => false, 'color' => '444444'], []);


        // Create Job Info Layout Table
        $jobInfoHeadingTable = $section->addTable(['width' => '100', 'borderSize' => 5, 'borderColor' => 'ffffff']);
        $jobInfoHeadingTable->addRow();
        $jobInfoTableHeaderCell = $jobInfoHeadingTable->addCell(ReportRepository::twip(18), ['bgColor' => $themeColor, 'valign' => 'center']);
        $jobInfoTableHeaderCell->addText('  JOB INFORMATION', ['bold' => true, 'color' => 'ffffff', 'size' => 14], ['space' => array('before' => 100, 'after' => 100)]);

        // Create Job Details Layout Table
        $jobInfoTable = $section->addTable(['width' => 100, 'borderSize' => 5, 'borderColor' => 'ffffff']);
        // Create Job Area Rows
        $jobInfoTable->addRow();
        $jobAreaInfoCell = $jobInfoTable->addCell(ReportRepository::twip(8), ['valign' => 'top']);
        $jobAreaInfoCell->addText('Job Area', ['color' => $themeColor, 'bold' => true], ['space' => array('before' => 0, 'after' => 0)]);
        $jobInvoiceCell = $jobInfoTable->addCell(ReportRepository::twip(10), ['valign' => 'top']);
        $jobInvoiceCell->addText('Invoice', ['color' => $themeColor, 'bold' => true], ['align' => 'right', 'space' => array('before' => 0, 'after' => 0)]);
        $jobInfoTable->addRow();
        $jobAreaInfoCell = $jobInfoTable->addCell(ReportRepository::twip(8), ['valign' => 'top']);
        foreach ($case->area as $area) {
            $jobAreaInfoCell->addText(ucfirst($area), ['color' => '444444', 'bold' => false]);
        }
        $jobAreaInvoiceCell = $jobInfoTable->addCell(ReportRepository::twip(10), ['valign' => 'top']);
        // Invoices
        if (isset($case->invoices)) {
            foreach ($case->invoices as $key => $invoice) {
                $invoice[$key] = $jobAreaInvoiceCell->addTextRun(['align' => 'right', 'space' => array('before' => 0, 'after' => 0)]);
                $invoice[$key]->addText($currencySymbol . $invoice['amount'], ['color' => '444444'], []);
                if (!empty($invoice['file']['s3key'])) {
                    $invoice[$key]->addLink(Helpers\Helpers::baseUrlToAppUrl() . '/document/' . base64_encode($invoice['file']['s3key']), ' - View Attachment', ['color' => $dangerColor], []);
                }
            }
        }
        // Create Job Asset Rows
        $jobInfoTable->addRow();
        $jobAssetInfoCell = $jobInfoTable->addCell(ReportRepository::twip(8), ['valign' => 'top']);
        $jobAssetInfoCell->addText('Asset', ['color' => $themeColor, 'bold' => true], ['space' => array('before' => 0, 'after' => 0)]);
        $jobQuotesCell = $jobInfoTable->addCell(ReportRepository::twip(10), ['valign' => 'top']);
        $jobQuotesCell->addText('Quotes', ['color' => $themeColor, 'bold' => true], ['align' => 'right', 'space' => array('before' => 0, 'after' => 0)]);
        $jobInfoTable->addRow();
        $jobAssetInfoCell = $jobInfoTable->addCell(ReportRepository::twip(8), ['valign' => 'top']);
        if (isset($case->assets)) {
            foreach ($case->assets as $asset) {
                $jobAssetInfoCell->addText($asset['name'], ['color' => '444444', 'bold' => false]);
            }
        }
        $jobAssetInvoiceCell = $jobInfoTable->addCell(ReportRepository::twip(10), ['valign' => 'top']);
        // Quotes
        if (isset($case->quotes)) {
            foreach ($case->quotes as $key => $quote) {
                $jobQuotes[$key] = $jobAssetInvoiceCell->addTextRun(['align' => 'right', 'space' => array('before' => 0, 'after' => 0)]);
                $jobQuotes[$key]->addText($currencySymbol . $quote['amount'], ['color' => '444444'], []);
                if (!empty($quote['file']['s3key'])) {
                    $jobQuotes[$key]->addLink(Helpers\Helpers::baseUrlToAppUrl() . '/document/' . base64_encode($quote['file']['s3key']), ' - View Attachment', ['color' => $dangerColor], []);
                }
            }
        }
        // Create Job Documents Rows
        $jobInfoTable->addRow();
        $jobDocInfoCell = $jobInfoTable->addCell(ReportRepository::twip(8), ['valign' => 'top']);
        $jobDocInfoCell->addText('   ', ['color' => $themeColor, 'bold' => true], ['space' => array('before' => 0, 'after' => 0)]);
        $jobAttachCell = $jobInfoTable->addCell(ReportRepository::twip(10), ['valign' => 'top']);
        $jobAttachCell->addText('Documents', ['color' => $themeColor, 'bold' => true], ['align' => 'right', 'space' => array('before' => 0, 'after' => 0)]);
        $jobInfoTable->addRow();
        $jobDocInfoCell = $jobInfoTable->addCell(ReportRepository::twip(8), ['valign' => 'top']);
        $jobDocInfoCell->addText('   ', ['color' => '444444', 'bold' => false]);
        $jobDocInvoiceCell = $jobInfoTable->addCell(ReportRepository::twip(10), ['valign' => 'top']);
        // Attachments
        $jobAttachments = $jobDocInvoiceCell->addTextRun(['align' => 'right', 'space' => array('before' => 0, 'after' => 0)]);
        if (isset($case->documents)) {
            foreach ($case->documents as $key => $d) {
                $jobAttachments->addText(($key + 1) . '. ', ['color' => '444444'], []);
                $jobAttachments->addLink(Helpers\Helpers::baseUrlToAppUrl() . '/document/' . base64_encode($d['file']['s3key']), 'View Attachment', ['color' => $linkColor], []);
            }
        }


        // Create Subject Layout Table
        $caseSubjectTable = $section->addTable(['width' => 100, 'borderSize' => 5, 'borderColor' => 'ffffff']);
        $caseSubjectTable->addRow();
        $caseSubjectHeaderCell = $caseSubjectTable->addCell(ReportRepository::twip(18), ['bgColor' => $themeColor, 'valign' => 'center']);
        $caseSubjectHeaderCell->addText('  SUBJECT', ['bold' => true, 'color' => 'ffffff', 'size' => 14], ['space' => array('before' => 100, 'after' => 100)]);
        $caseSubjectTable->addRow();
        $caseSubjectCell = $caseSubjectTable->addCell(ReportRepository::twip(18), ['valign' => 'center']);
        $caseSubjectCell->addText($case->subject, ['bold' => false, 'color' => '444444'], ['space' => array('before' => 50, 'after' => 50)]);
        $caseSubjectCell->addText('', ['bold' => false, 'color' => '444444'], ['space' => array('before' => 50, 'after' => 50)]);


        // Create Description Layout Table
        $caseDescriptionTable = $section->addTable(['width' => 100, 'borderSize' => 5, 'borderColor' => 'ffffff']);
        $caseDescriptionTable->addRow();
        $caseDescriptionHeaderCell = $caseDescriptionTable->addCell(ReportRepository::twip(18), ['bgColor' => $themeColor, 'valign' => 'center']);
        $caseDescriptionHeaderCell->addText('  JOB DESCRIPTION', ['bold' => true, 'color' => 'ffffff', 'size' => 14], ['space' => array('before' => 100, 'after' => 100)]);
        $caseDescriptionTable->addRow();
        $caseDescriptionCell = $caseDescriptionTable->addCell(ReportRepository::twip(18), ['valign' => 'center']);
        @Html::addHtml($caseDescriptionCell, $case->detail, false, false);
        $caseDescriptionCell->addText('', ['bold' => false, 'color' => '444444'], ['space' => array('before' => 50, 'after' => 50)]);


        // Create Media Layout Table
        $caseMediaTable = $section->addTable(['width' => 100, 'borderSize' => 5, 'borderColor' => 'ffffff']);
        $caseMediaTable->addRow();
        $caseMediaHeaderCell = $caseMediaTable->addCell(ReportRepository::twip(18), ['bgColor' => $themeColor, 'valign' => 'center']);
        $caseMediaHeaderCell->addText('  MEDIA', ['bold' => true, 'color' => 'ffffff', 'size' => 14], ['space' => array('before' => 100, 'after' => 100)]);
        $caseMediaTable->addRow();
        $caseMediaAttachCell = $caseMediaTable->addCell(ReportRepository::twip(18), ['gridSpan' => 2]);
        $caseMediaAttachCell = $caseMediaAttachCell->addTextRun(
            ['textAlignment' => 'center', 'alignment' => 'start']
        );
        $innerWidth = ReportRepository::point(18);
        $imgWidth = ($innerWidth - 50) / 3;
        $imgHeight = ($innerWidth - 50) / 4;
        if (isset($case->photos)) {
            foreach ($case->photos as $photo) {
                if (!empty($photo['file']['s3key'])) {
                    $secure_url = $s3Bucket->preSecureDocumentUrl($photo['file']['s3key']);
                    $fileContent = file_get_contents($secure_url);
                    $file_path = MybosFile::createFile('imageFile', $fileContent);
                    $caseMediaAttachCell->addImage(
                        $file_path,
                        [
                            'width' => $imgWidth,
                            'height' => $imgHeight
                        ],
                        false,
                        $photo['file']['file_name']
                    );
                    $caseMediaAttachCell->addText('    ');
                }
            }
        }

        $caseMediaAttachCell->addText('', ['bold' => false, 'color' => '444444'], ['space' => array('before' => 50, 'after' => 50)]);


        // Create Terms Layout Table
        $caseTermsTable = $section->addTable(['width' => 100, 'borderSize' => 5, 'borderColor' => 'ffffff']);
        $caseTermsTable->addRow();
        $caseTermsHeaderCell = $caseTermsTable->addCell(ReportRepository::twip(18), ['bgColor' => $themeColor, 'valign' => 'center']);
        $caseTermsHeaderCell->addText('  TERMS CONDITIONS', ['bold' => true, 'color' => 'ffffff', 'size' => 14], ['space' => array('before' => 100, 'after' => 100)]);
        $caseTermsTable->addRow();
        $caseTermsValueCell = $caseTermsTable->addCell(ReportRepository::twip(18), ['valign' => 'center']);
        if (!empty($building['attributes']['work_order_settings']) && $building['attributes']['work_order_settings']['wo_disclaimer_enable'] == 1) {
            $woDisclaimerBody = MybosString::sanitizeForWordHtml($building['attributes']['work_order_settings']['wo_disclaimer'] ?? '');
            @Html::addHtml($caseTermsValueCell, $woDisclaimerBody, false, false);
        }
        $caseTermsValueCell->addText('', ['bold' => false, 'color' => '444444'], ['space' => array('before' => 50, 'after' => 50)]);

        // Export DOC file
        \PhpOffice\PhpWord\Settings::setOutputEscapingEnabled(true);
        $phpWord = $document->getObject();
        $objWriter = \PhpOffice\PhpWord\IOFactory::createWriter($phpWord, 'Word2007');
        header('Access-Control-Allow-Origin: *');
        $objWriter->save('php://output');
    }

    /**
     * @param string $statusId
     * @param Cases $case
     * @param Building $building
     */
    public static function changeStatus(string $statusId, Cases $case, Building $building): array|Cases
    {
        $dateNow = MybosTime::dbCompatible(MybosTime::now());
        $statusCompleted = Category::where('building_id', $building['id'])
            ->where('type', MybosCategoryType::CASE_STATUS)
            ->where('name', MybosBaseCaseStatus::COMPLETED)
            ->first();
        if (!$statusCompleted instanceof Category) {
            return ['error' => 'Cases status update failed. Cannot find building\'s "' . MybosBaseCaseStatus::COMPLETED . '" status.'];
        }

        if ($statusId == $statusCompleted->_id) {
            $case->completion_date = $dateNow;
        }
        $case->status_id = $statusId;

        if (!$case->save()) {
            return ['error' => 'Cases status update failed'];
        }

        return $case;
    }

    /**
     * @param Building $building
     * @param array $caseIds
     * @return bool|string[]
     */
    public static function setCaseStatusToCompleted(Building $building, array $caseIds = []): array|bool
    {
        $status = Category::where('building_id', $building['id'])
            ->where('type', MybosCategoryType::CASE_STATUS)
            ->where('name', MybosBaseCaseStatus::COMPLETED)
            ->first();
        if (!$status instanceof Category) {
            return ['error' => 'Cases status update failed. Cannot find building\'s "' . MybosBaseCaseStatus::COMPLETED . '" status.'];
        }

        foreach ($caseIds as $caseId) {
            $case = Cases::find($caseId);
            if (!$case instanceof Cases) {
                return ['error' => 'Cases status update prematurely halted. Cannot find case(' . $caseId . ').'];
            }

            if ((string)$case['building_id'] != (string)$building['_id']) {
                return ['error' => 'Cases status update prematurely halted. Case(' . $caseId . ') does not belong to this building.'];
            }

            // no need to update updated_at here as it is already done in model.
            $case->status_id = $status->_id;
            $case->completion_date = MybosTime::dbCompatible(MybosTime::now());
            if (!$case->save()) {
                return ['error' => 'Cases status update prematurely halted. Cannot update case(' . $caseId . ') status to ' . MybosBaseCaseStatus::COMPLETED . '.'];
            }
        }

        return true;
    }

    /**
     * @param Building $building
     * @param array $caseIds
     * @return bool|string[]
     */
    public static function setCaseStatusToDeleted(Building $building, array $caseIds = []): array|bool
    {
        $statusDeleted = Category::where('building_id', $building['id'])
            ->where('type', MybosCategoryType::CASE_STATUS)
            ->where('name', MybosBaseCaseStatus::DELETED)
            ->first();
        if (!$statusDeleted instanceof Category) {
            return ['error' => 'Cases status update failed. Cannot find building\'s "' . MybosBaseCaseStatus::DELETED . '" status.'];
        }

        foreach ($caseIds as $caseId) {
            $case = Cases::find($caseId);
            if (!$case instanceof Cases) {
                return ['error' => 'Cases status update prematurely halted. Cannot find case(' . $caseId . ').'];
            }

            if ((string)$case['building_id'] != (string)$building['_id']) {
                return ['error' => 'Cases status update prematurely halted. Case(' . $caseId . ') does not belong to this building.'];
            }

            // no need to update updated_at here as it is already done in model.
            $case->status_id = $statusDeleted->_id;
            if (!$case->save()) {
                return ['error' => 'Cases status update prematurely halted. Cannot update case(' . $caseId . ') status to ' . MybosBaseCaseStatus::COMPLETED . '.'];
            }
        }
        return true;
    }
    public static function setCaseStatusToInprogress(Building $building, array $caseIds = []): array|bool
    {
        $statusInprogress = Category::where('building_id', $building['id'])
            ->where('type', MybosCategoryType::CASE_STATUS)
            ->where('name', MybosBaseCaseStatus::IN_PROGRESS)
            ->first();
        if (!$statusInprogress instanceof Category) {
            return ['error' => 'Cases status update failed. Cannot find building\'s "' . MybosBaseCaseStatus::IN_PROGRESS . '" status.'];
        }

        foreach ($caseIds as $caseId) {
            $case = Cases::find($caseId);
            if (!$case instanceof Cases) {
                return ['error' => 'Cases status update prematurely halted. Cannot find case(' . $caseId . ').'];
            }

            // no need to update updated_at here as it is already done in model.
            $case->status_id = $statusInprogress->_id;
            if (!$case->save()) {
                return ['error' => 'Cases status update prematurely halted. Cannot update case(' . $caseId . ') status to ' . MybosBaseCaseStatus::COMPLETED . '.'];
            }
        }
        return true;
    }

    /**
     * =====================
     * Update star
     * =====================
     */
    public static function UpdateStarred($starValue, $case)
    {
        $case->starred = (int)$starValue;
        if ($case->save()) {
            return $case;
        }
        return ['error' => 'Can not update stared'];
    }

    /**
     * ========================
     * Case move to folder
     * ========================
     */
    public static function FolderMove($request)
    {
        $input = $request->input();
        foreach ($input['case_folder'] as $caseFolderMove) {
            $case = Cases::find($caseFolderMove['case_id']);
            if ($case instanceof Cases) {
                $objectFolderID = [];
                foreach ($caseFolderMove['folder_id'] as $folderId) {
                    $objectFolderID[] = new ObjectId($folderId);
                }
                $case->folders = $objectFolderID;
                $case->save();
            }
        }
        return ['status' => 200, 'msg' => 'Cased moved to folders successfully.'];
    }


    /**
     * @param $caseDataEmails
     * @param Cases $case
     * @param User $user
     * @param Building $building
     * @param array $localeProfile
     * @return array[]
     */
    public static function sendTemplatedCaseEmails($caseDataEmails, Cases $case, User $user, Building $building, array $localeProfile): array
    {
        // Set primary case linked data(ex: building, company, user...).
        $case['session_user'] = $user;
        $case['building_details'] = $building;
        $case['plan'] = $building['plan'];

        // Get building work order settings.
        $work_order_entity_details = $building['attributes']['work_order_settings']['entity_details'] ?? [];
        $case['building_name'] = !empty($work_order_entity_details['building_name']) ? $work_order_entity_details['building_name'] : ($building['name'] ?? '');
        $case['building_address'] = !empty($work_order_entity_details['address']) ? $work_order_entity_details['address'] : ($building['address']['street_address'] ?? '');
        $case['post_code'] = $work_order_entity_details['post_code'] ?? '';
        $case['state'] = $work_order_entity_details['state'] ?? '';
        $case['suburb'] = $work_order_entity_details['suburb'] ?? '';
        $case['sp'] = $work_order_entity_details['sp'] ?? '';

        // Set building's company entity_name with fallbacks.
        $companyEntityName = !empty(trim($work_order_entity_details['company_name'])) ? (trim($work_order_entity_details['company_name'])) : '';
        if (empty($companyEntityName)) {
            $buildingPrimaryCompany = $building->getPrimaryCompany();
            if ($buildingPrimaryCompany instanceof Company) {
                $companyEntityName = $buildingPrimaryCompany['entity_name'] ?? '';
            }
        }
        $case['company_name'] = $companyEntityName ?? '';

        $extraCaseData = [
            'logo_path' => $caseDataEmails['logo_path'],
            'description' => $caseDataEmails['description'] ?? 0,
            'photo' => $caseDataEmails['photo'] ?? 0,
            'document' => $caseDataEmails['document'] ?? 0,
            'invoice' => $caseDataEmails['invoice'] ?? 0,
            'quote' => $caseDataEmails['quote'] ?? 0,
        ];

        // Get building case template and fallback to an empty template if it fails.
        $emailTemplateName = match ($caseDataEmails['type']) {
            'work order' => MybosMessaging::_CASE_WORK_ORDER_EMAIL,     // 'work order'
            'quote request' => MybosMessaging::_CASE_QUOTE_EMAIL,       // 'quote request'
            default => MybosMessaging::_CASE_SUMMARY_EMAIL,             // 'summary'
        };
        $mybosMessagingCaseEmailTemplate = $building->getEmailTemplateSettingData(MybosMessaging::CASE, $emailTemplateName);
        if (!$mybosMessagingCaseEmailTemplate instanceof EmailTemplateSettingsData) {
            $mybosMessagingCaseEmailTemplate = new EmailTemplateSettingsData();
        }

        // Set temporary file to save PDf.
        $strTempFilePath = sys_get_temp_dir() . '/case_' . $case['number'] . '_' . Helpers\MybosString::generateRandomCharacters(6) . '.pdf';

        // Initialise logs for storing skipped emails.
        $skippedEmails = [];
        $sentEmails = [];

        $caseContractorContactEmails = $caseDataEmails['contact_emails'] ?? [];
        foreach ($caseContractorContactEmails as $caseContactEmail) {
            $contractorEntity = Contractor::find($caseContactEmail['contractor_id']);
            if (!$contractorEntity instanceof Contractor) {
                $skippedEmails[] = $caseContactEmail['contact_email'];
                continue;
            }
            $case['case_contractors'] = $contractorEntity;

            // Get email reference number.
            $emailReferenceNumber = $caseContactEmail['reference_number'];    //or use use this:"$case->getNextEmailReference($index);" if to generate new one and disregard the one from frontend.
            $case['reference_number'] = $emailReferenceNumber;

            // Set flag for showing "WORK ORDER DESCRIPTION" in pdfs...
            $work_order_description = false;
            if (!empty($message)) {
                $work_order_description = true;
            }

            // Generate/Get contractor short-code.
            $shortCodeContractorLink = $case->generateCaseContractorLink($contractorEntity['_id'], $caseContactEmail['contact_id'], $work_order_description);
            $case['link'] = Helpers\Helpers::baseUrlToAppUrl() . '/c/' . $shortCodeContractorLink;

            $case['allow_access'] = $caseContactEmail['allow_access'] ?? 0;
            $case['type_str'] = $caseDataEmails['type'];
            $case['due_by'] = date($localeProfile['date_format'], strtotime($caseDataEmails['due_by']));

            // Set PDF color.
            $case['color'] = match ($caseDataEmails['type']) {
                MybosCaseMailType::QUOTE_REQUEST => '#F29E58',
                MybosCaseMailType::SUMMARY => '#00B0F0',
                default => '#8EC26A',   //MybosCaseMailType::WORK_ORDER,
            };
            $case['currencySymbol'] = Currency::find($building->locale->selected_country_profile->currency_code)->symbol;
            // Generate PDF...
            $pdf = PDF::loadView('PDF.case', ['case' => $case, 'data' => $extraCaseData, 'work_order_description' => $work_order_description]);
            $pdf->save($strTempFilePath);

            /***************** *********************/
            /** Set email data for email template **/
            /***************** *********************/
            // Get mail signature from bm-user profile..
            $signature = $user->getBuildingMailProfileSignature($building, true);

            // Set email template's subject and body as passed from frontend because the case template may have been altered by the sender just before sending the email.
            if (!empty($caseContactEmail['subject'])) {
                $mybosMessagingCaseEmailTemplate->subject = $caseContactEmail['subject'];
            }
            if (!empty($caseContactEmail['message'])) {
                $mybosMessagingCaseEmailTemplate->body = $caseContactEmail['message'];
            }

            // Assemble email placeholder replacement values.
            $placeHolderReplacements = [
                "{Number}" => $case['number'],
                "{BuildingName}" => $case['building_name'],
                "{ContactName}" => $caseContactEmail['contact_name'],
            ];

            // Create and send mybos mailable email.
            $mailableStandardTemplatedEmail = new StandardTemplatedEmail(
                $placeHolderReplacements, $mybosMessagingCaseEmailTemplate, $building, $signature,
                [
                    [
                        'path' => $strTempFilePath,
                        'filename' => $case['subject'].' Ref_' . $emailReferenceNumber . '.pdf',
                        'type' => 'application/pdf',
                    ]
                ]);
            $cc = $caseContactEmail['cc'] ?? [];
            $bcc = $caseContactEmail['bcc'] ?? [];
            if ($mailableStandardTemplatedEmail->sendToEmail($caseContactEmail['contact_email'], $caseContactEmail['contact_name'], $user, $cc, $bcc)) {
                $sentEmails[] = $caseContactEmail['contact_email'];
            } else {
                $skippedEmails[] = $caseContactEmail['contact_email'];
            }

            // update placeholder before save to DB
            $placeholders = array_keys($placeHolderReplacements);
            $replacements = array_values($placeHolderReplacements);
            $emailSubject = str_replace($placeholders, $replacements, $mybosMessagingCaseEmailTemplate->subject);
            $emailBody = str_replace($placeholders, $replacements, $mybosMessagingCaseEmailTemplate->body);

            // Save case email data.
            $caseEmailData = new CaseEmailData();
            $caseEmailData->number = $emailReferenceNumber;
            $caseEmailData->sender_id = $user['_id'];
            $caseEmailData->contractor_id = $caseContactEmail['contractor_id'];
            $caseEmailData->contact_id = $caseContactEmail['contact_id'];
            $caseEmailData->type = $caseDataEmails['type'];
            $caseEmailData->due_by = MybosTime::CarbonToUTCDateTime(Carbon::createFromFormat('Y-m-d', $caseDataEmails['due_by'], $building['timezone']));
            $caseEmailData->subject = $emailSubject;    //$emailSubject;
            $caseEmailData->message = $emailBody;    //$message;

            // Save/attach pdf file.
            $s3File = AwsS3::uploadAsFileType($strTempFilePath, MybosFileType::CASE_DOCUMENT, $case);
            $caseEmailData->attachments = ($s3File instanceof S3FileModel) ? [$s3File] : [];

            $caseEmail = new CaseEmail($caseEmailData->toArray());
            $case->emails()->save($caseEmail);

        }   // END of foreach()...


        return [
            'sent_emails' => $sentEmails,
            'skipped_emails' => $skippedEmails,
            'user' => $user,
            'userProfileMailSignature' => $signature
        ];
    }

    /**
     * Send email notification for "Contractor Portal" specific updates.
     *
     * @param string $mybosMessagingContractorEmailTemplate
     * @param Cases $case
     * @param Contractor $contractor
     * @param $contractorContact
     * @param Building $building
     * @return bool
     */
    public static function contractorPortalSendNotificationEmail(string $mybosMessagingContractorEmailTemplate, Cases $case, Contractor $contractor, $contractorContact, Building $building): bool
    {
        $skippedEmails = [];
        $sentEmails = [];
        $contractorContactName = $contractorContact != null ? $contractorContact[0]['name'] : '';

        // Get MybosMessaging [contractor] email template. ex: MybosMessaging::_CONTRACTOR_UPLOADS_DOCUMENT
        $mybosMessagingTemplate = $building->getEmailTemplateSettingData(MybosMessaging::CONTRACTOR, $mybosMessagingContractorEmailTemplate);
        if (!$mybosMessagingTemplate instanceof EmailTemplateSettingsData || !$mybosMessagingTemplate->status) {
            return false;
        }

        // Loop through all BM/manager's notification emails
        $contractorManagerNotificationEmails = $building->getSectionEmailNotifications(MybosMessaging::CONTRACTOR);
        foreach ($contractorManagerNotificationEmails as $managerEmail) {
            // Assemble the placeholder replacement values.
            $placeHolderReplacements = [
                "{Company}" => $contractor['company_name'],
                "{User}" => $contractorContactName,
                "{CaseNo}" => $case['number'],
                "{CaseSubject}" => $case['subject'],
            ];

            // Create and send mybos mailable email.
            $mailableStandardTemplatedEmail = new StandardTemplatedEmail($placeHolderReplacements, $mybosMessagingTemplate, $building);
            if ($mailableStandardTemplatedEmail->sendToEmail($managerEmail, 'Manager')) {
                $sentEmails[] = $managerEmail;
            } else {
                $skippedEmails[] = $managerEmail;
            }
        }


        // Todo: Ask Sam if we need to send notification to requesting [resident] here if this case is created from
        //       a maintenance_request. See CasesController's updateCaseInfo() function... -Lino

        // Log skipped emails. (For future project: log these into database so building managers can see/view.) -Lino
        if (!empty($skippedEmails)) {
            Log::warning('CaseRepository::contractorPortalSendNotificationEmail(): skipped emails...', $skippedEmails);
        }

        // Log if no emails are sent. (For future project: log these into database so building managers can see/view.) -Lino
        if (empty($sentEmails)) {
            Log::warning('CaseRepository::contractorPortalSendNotificationEmail(): no emails sent...', $contractorManagerNotificationEmails);
        }

        return true;
    }

    public static function getCaseInventoryUsages(string $caseId)
    {
        $mdbCasesCollection = app('MongoDbClient')->getCollection('cases');

        $dbFilter = [
            [
                '$match' => ['_id' => new ObjectId($caseId)]
            ],
            //[ '$project' => ['subject' => 1, 'detail' => 1] ],
            [
                // [Inventory Usages s1] : convert inventory_id string to objectId
                '$addFields' => [
                    'case_inventory_usage' => [
                        '$map' => [
                            'input' => '$inventory_usages',
                            'in' => [
                                '$mergeObjects' => [
                                    '$$this',
                                    [
                                        'inventory_id' => [
                                            '$toObjectId' => '$$this.inventory_id'
                                        ]
                                    ]

                                ]
                            ]
                        ]
                    ]
                ]
            ],
            [
                // [Inventory Usages s2] : lookup inventory based on [inventory_usage_details] converted contractor_id above.
                '$lookup' => [
                    'from' => 'inventories',
                    'localField' => 'case_inventory_usage.inventory_id',
                    'foreignField' => '_id',
                    'pipeline' => [
                        [
                            '$project' => [
                                'item' => 1, 'location' => 1, 'description' => 1, 'owner' => 1, 'serial' => 1,
                                'value' => 1, 'sell' => 1, 'quantity' => 1, 'unlimited' => 1,
                            ]
                        ]
                    ],
                    'as' => 'inventory_details',
                ]
            ],
            [
                // [Inventory Usages s3] : do $addFields here to filter out more fields to remove later below on the projection.
                '$addFields' => [
                    'inventory_usages' => [
                        '$map' => [
                            'input' => '$case_inventory_usage',
                            'as' => 'ciu',
                            'in' => [
                                '$mergeObjects' => [
                                    '$$ciu',
                                    [
                                        'inventory' => [       // Add inventory details as embedded data. (Note: Alternatively, remove this to actually do a merge instead of embedding)
                                            '$first' => [
                                                '$filter' => [
                                                    'input' => '$inventory_details',
                                                    'cond' => [
                                                        '$eq' => ['$$this._id', '$$ciu.inventory_id']
                                                    ]
                                                ]
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ],
            [
                '$project' => [
                    'inventory_usages' => 1,
                ]
            ]
        ];

        $resContacts = $mdbCasesCollection->aggregate(
            $dbFilter
        );

        $caseWithQuotesFullDetails = $resContacts->toArray();
        $inventoryUsages = reset($caseWithQuotesFullDetails);

        /** Get formatted dates **/
        $case = Cases::find($caseId);
        foreach ($inventoryUsages['inventory_usages'] as $usage) {
            $quote['created_at'] = !empty($quote['created_at']) ? MybosTime::format_mongoDbTimeUsingBuildingTimezone($quote['created_at'], $case->building->timezone, MybosDateTimeFormat::STANDARD) : '';
            $quote['updated_at'] = !empty($quote['updated_at']) ? MybosTime::format_mongoDbTimeUsingBuildingTimezone($quote['updated_at'], $case->building->timezone, MybosDateTimeFormat::STANDARD) : '';
        }

        return $inventoryUsages['inventory_usages'];
    }

    public static function getCaseQuotes(string $caseId)
    {
        $mdbCasesCollection = app('MongoDbClient')->getCollection('cases');

        $dbFilter = [
            [
                '$match' => ['_id' => new ObjectId($caseId)]
            ],
            //[ '$project' => ['subject' => 1, 'detail' => 1] ],
            [
                // [Quotes s1] : convert contractor_id string to objectId
                '$addFields' => [
                    'quotes_details' => [
                        '$map' => [
                            'input' => '$quotes',
                            'in' => [
                                '$mergeObjects' => [
                                    '$$this',
                                    [
                                        'contractor_id' => [
                                            '$toObjectId' => '$$this.contractor_id'
                                        ]
                                    ]

                                ]
                            ]
                        ]
                    ]
                ]
            ],
            [
                // [Quotes s2] : lookup contractors based on [emails_details] converted contractor_id above.
                '$lookup' => [
                    'from' => 'contractors',
                    'localField' => 'quotes_details.contractor_id',
                    'foreignField' => '_id',
                    'pipeline' => [
                        [
                            '$project' => [
                                'company_name' => 1, 'website' => 1
                            ]
                        ]
                    ],
                    'as' => 'quotes_contractor_details',
                ]
            ],
            [
                // [Quotes s3] : do $addFields here to filter out more fields to remove later below on the projection.
                '$addFields' => [
                    'quotes' => [
                        '$map' => [
                            'input' => '$quotes_details',
                            'as' => 'ed',
                            'in' => [
                                '$mergeObjects' => [
                                    '$$ed',
                                    [
                                        'contractor' => [       // Add contractor details as embedded data. Remove this to actually do a merge instead of embedding.
                                            '$first' => [
                                                '$filter' => [
                                                    'input' => '$quotes_contractor_details',
                                                    'cond' => [
                                                        '$eq' => ['$$this._id', '$$ed.contractor_id']
                                                    ]
                                                ]
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ],
            [
                '$project' => [
                    'quotes._id' => 1,
                    'quotes.number' => 1,
                    'quotes.amount' => 1,
                    'quotes.fee' => 1,
                    'quotes.status' => 1,
                    'quotes.comment' => 1,
                    'quotes.contractor_id' => 1,
                    'quotes.approval_date' => 1,
                    'quotes.created_at' => 1,
                    'quotes.file._id' => 1,
                    'quotes.file.file_name' => 1,
                    'quotes.file.s3key' => 1,
                    'quotes.file.created_at' => 1,
                    'quotes.contractor' => 1,
                    '_id' => -1
                ]
            ]
        ];

        $resContacts = $mdbCasesCollection->aggregate(
            $dbFilter
        );

        $caseWithQuotesFullDetails = $resContacts->toArray();
        $caseQuotes = reset($caseWithQuotesFullDetails);

        /** Get formatted dates and pre-secured url for all images/files **/
        $s3Bucket = new AwsS3();
        $case = Cases::find($caseId);
        foreach ($caseQuotes['quotes'] as $quote) {
            $quote['created_at_8601'] = !empty($quote['created_at']) ? MybosTime::UTCDateTimeToCarbon($quote['created_at']) : '';
            $quote['created_at'] = !empty($quote['created_at']) ? MybosTime::format_mongoDbTimeUsingBuildingTimezone($quote['created_at'], $case->building->timezone, MybosDateTimeFormat::STANDARD) : '';
            if (!empty($quote['file']) && !empty($quote['file']['created_at'])) {
                $quote['file']['created_at_8601'] = !empty($quote['file']['created_at']) ? MybosTime::UTCDateTimeToCarbon($quote['file']['created_at']) : '';
                $quote['file']['created_at'] = !empty($quote['file']['created_at']) ? MybosTime::format_mongoDbTimeUsingBuildingTimezone($quote['file']['created_at'], $case->building->timezone, MybosDateTimeFormat::STANDARD) : '';
            }
            if (!empty($quote['file']) && !empty($quote['file']['s3key'])) {
                $quote['file']['secure_url'] = $s3Bucket->preSecureDocumentUrl($quote['file']['s3key']);
            }
        }

        return $caseQuotes['quotes'];
    }

    public static function getCaseInvoices(string $caseId)
    {
        $mdbCasesCollection = app('MongoDbClient')->getCollection('cases');

        $dbFilter = [
            [
                '$match' => ['_id' => new ObjectId($caseId)]
            ],
            //[ '$project' => ['subject' => 1, 'detail' => 1] ],
            [
                // [Invoices s1] : convert contractor_id string to objectId
                '$addFields' => [
                    'invoices_details' => [
                        '$map' => [
                            'input' => '$invoices',
                            'in' => [
                                '$mergeObjects' => [
                                    '$$this',
                                    [
                                        'contractor_id' => [
                                            '$toObjectId' => '$$this.contractor_id'
                                        ]
                                    ]

                                ]
                            ]
                        ]
                    ]
                ]
            ],
            [
                // [Invoices s2] : lookup contractors based on [emails_details] converted contractor_id above.
                '$lookup' => [
                    'from' => 'contractors',
                    'localField' => 'invoices_details.contractor_id',
                    'foreignField' => '_id',
                    'pipeline' => [
                        [
                            '$project' => [
                                'company_name' => 1, 'website' => 1
                            ]
                        ]
                    ],
                    'as' => 'invoices_contractor_details',
                ]
            ],
            [
                // [Invoices s3] : do $addFields here to filter out more fields to remove later below on the projection.
                '$addFields' => [
                    'invoices' => [
                        '$map' => [
                            'input' => '$invoices_details',
                            'as' => 'ed',
                            'in' => [
                                '$mergeObjects' => [
                                    '$$ed',
                                    [
                                        'contractor' => [       // Add contractor details as embedded data. Remove this to actually do a merge instead of embedding.
                                            '$first' => [
                                                '$filter' => [
                                                    'input' => '$invoices_contractor_details',
                                                    'cond' => [
                                                        '$eq' => ['$$this._id', '$$ed.contractor_id']
                                                    ]
                                                ]
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ],
            [
                '$project' => [
                    'invoices._id' => 1,
                    'invoices.number' => 1,
                    'invoices.base_amount' => 1,
                    'invoices.amount' => 1,
                    'invoices.fee' => 1,
                    'invoices.status' => 1,
                    'invoices.comment' => 1,
                    'invoices.contractor_id' => 1,
                    'invoices.budget_id' => 1,
                    'invoices.approval_date' => 1,
                    'invoices.created_at' => 1,
                    'invoices.file._id' => 1,
                    'invoices.file.file_name' => 1,
                    'invoices.file.s3key' => 1,
                    'invoices.file.created_at' => 1,
                    'invoices.contractor' => 1,
                    '_id' => -1
                ]
            ]
        ];

        $resContacts = $mdbCasesCollection->aggregate(
            $dbFilter
        );

        $caseWithInvoicesFullDetails = $resContacts->toArray();
        $caseInvoices = reset($caseWithInvoicesFullDetails);

        /** Get formatted dates and pre-secured url for all images/files **/
        $s3Bucket = new AwsS3();
        $case = Cases::find($caseId);
        foreach ($caseInvoices['invoices'] as $invoice) {
            $invoice['created_at'] = !empty($invoice['created_at']) ? MybosTime::format_mongoDbTimeUsingBuildingTimezone($invoice['created_at'], $case->building->timezone, MybosDateTimeFormat::STANDARD) : '';
            if (!empty($invoice['file']) && !empty($invoice['file']['created_at'])) {
                $invoice['file']['created_at'] = !empty($invoice['file']['created_at']) ? MybosTime::format_mongoDbTimeUsingBuildingTimezone($invoice['file']['created_at'], $case->building->timezone, MybosDateTimeFormat::STANDARD) : '';
            }
            if (!empty($invoice['file']) && !empty($invoice['file']['s3key'])) {
                $invoice['file']['secure_url'] = $s3Bucket->preSecureDocumentUrl($invoice['file']['s3key']);
            }
        }

        return $caseInvoices['invoices'];
    }

    public static function getCaseFullDetails(string $caseId)
    {
        $mdbCasesCollection = app('MongoDbClient')->getCollection('cases');

        $dbFilter = [
            [
                '$match' => [
                    '_id' => new ObjectId($caseId),
                    'deleted_at' => null
                ]
            ],
            //[ '$project' => ['subject' => 1, 'detail' => 1] ],
            [
                // [Apartments]
                '$lookup' => [
                    'from' => 'apartments',
                    'localField' => 'apartments',
                    'foreignField' => '_id',
                    'pipeline' => [
                        [
                            '$match' => [
                                'deleted_at' => ['$eq' => null],
                            ]
                        ],
                        [
                            '$project' => [
                                'unit_label' => 1, 'lot' => 1, 'is_hotel' => 1, 'status' => 1,
                                '_id' => ['$toString' => '$_id']
                            ]
                        ],
                    ],
                    'as' => 'apartments',
                ]
            ],
            [
                // [Status - Categories]
                '$lookup' => [
                    'from' => 'categories',
                    'let' => [
                        'statusId' => ['$toObjectId' => '$status_id']
                    ],
                    'pipeline' => [
                        [
                            '$match' => [
                                '$expr' => [
                                    '$eq' => ['$$statusId', '$_id']
                                ]
                            ]
                        ],
                        [
                            '$project' => [
                                'name' => 1, 'type' => 1, 'management_name' => 1
                            ]
                        ]
                    ],
                    'as' => 'status'
                ]
            ],
            [
                // [Type - Categories]
                '$lookup' => [
                    'from' => 'categories',
                    'let' => [
                        'typeId' => MongoDbNative::_convertToNullOnError('$type_id', 'objectId')
                    ],
                    'pipeline' => [
                        [
                            '$match' => [
                                '$expr' => [
                                    '$eq' => ['$$typeId', '$_id']
                                ]
                            ]
                        ],
                        [
                            '$project' => [
                                'name' => 1, 'type' => 1
                            ]
                        ]
                    ],
                    'as' => 'type'
                ]
            ],
            [
                // [Priority - Categories]
                '$lookup' => [
                    'from' => 'categories',
                    'let' => [
                        'priorityId' => MongoDbNative::_convertToNullOnError('$priority_id', 'objectId')
                    ],
                    'pipeline' => [
                        [
                            '$match' => [
                                '$expr' => [
                                    '$eq' => ['$$priorityId', '$_id']
                                ]
                            ]
                        ],
                        [
                            '$project' => [
                                'name' => 1, 'type' => 1
                            ]
                        ]
                    ],
                    'as' => 'priority'
                ]
            ],
            [
                '$unwind' => [
                    'path' => '$status',
                    'preserveNullAndEmptyArrays' => true
                ]
            ],
            [
                '$unwind' => [
                    'path' => '$type',
                    'preserveNullAndEmptyArrays' => true
                ]
            ],
            [
                '$unwind' => [
                    'path' => '$priority',
                    'preserveNullAndEmptyArrays' => true
                ]
            ],
            [
                // [Contractors]
                '$lookup' => [
                    'from' => 'contractors',
                    'localField' => 'contractors.contractor_id',
                    'foreignField' => '_id',
                    'pipeline' => [
                        [
                            '$project' => [
                                'company_name' => 1, 'website' => 1, 'phone' => 1, 'email' => 1, 'contacts' => 1
                            ]
                        ]
                    ],
                    'as' => 'contractors_details',
                ]
            ],
            [
                // [Folders]
                '$lookup' => [
                    'from' => 'folders',
                    'localField' => 'folders',
                    'foreignField' => '_id',
                    'pipeline' => [
                        [
                            '$project' => [
                                'name' => 1, 'type' => 1
                            ]
                        ]
                    ],
                    'as' => 'folders_details',
                ]
            ],
            [
                // [Assets]
                '$lookup' => [
                    'from' => 'assets',
                    'localField' => 'assets',
                    'foreignField' => '_id',
                    'pipeline' => [
                        [
                            '$project' => [
                                'name' => 1, 'description' => 1, '_id' => ['$toString' => '$_id']
                            ]
                        ]
                    ],
                    'as' => 'assets_details',
                ]
            ],
            [
                // [Email s0] Sort emails by descending date.
                '$addFields' => [
                    'emails_sorted_desc_date' => [
                        '$sortArray' => [
                            'input' => '$emails',
                            'sortBy' => ['created_at' => -1]
                        ]
                    ]
                ]
            ],
            [
                // [Emails s1] : convert contractor_id string to objectId
                '$addFields' => [
                    'emails_details' => [
                        '$map' => [
                            'input' => [
                                // Todo: add endpoint to get further email history by batches - Lino.
                                // Limit total emails to show to 100 only.
                                '$slice' => [
                                    '$emails_sorted_desc_date', 100
                                ]
                            ],
                            'in' => [
                                '$mergeObjects' => [
                                    '$$this',
                                    [
                                        'contractor_id' => MongoDbNative::_convertToNullOnError('$$this.contractor_id', 'objectId')
                                    ]

                                ]
                            ]
                        ]
                    ]
                ]
            ],
            [
                // [Emails s2] : lookup contractors based on [emails_details] converted contractor_id above.
                '$lookup' => [
                    'from' => 'contractors',
                    'localField' => 'emails_details.contractor_id',
                    'foreignField' => '_id',
                    'pipeline' => [
                        [
                            '$project' => [
                                'company_name' => 1, 'website' => 1
                            ]
                        ]
                    ],
                    'as' => 'emails_contractor_details',
                ]
            ],
            [
                // [Quotes s1] : convert contractor_id string to objectId
                '$addFields' => [
                    'quotes_details' => [
                        '$map' => [
                            'input' => '$quotes',
                            'in' => [
                                '$mergeObjects' => [
                                    '$$this',
                                    [
                                        'contractor_id' => MongoDbNative::_convertToNullOnError('$$this.contractor_id', 'objectId')
                                    ]

                                ]
                            ]
                        ]
                    ]
                ]
            ],
            [
                // [Quotes s2] : lookup contractors based on [emails_details] converted contractor_id above.
                '$lookup' => [
                    'from' => 'contractors',
                    'localField' => 'quotes_details.contractor_id',
                    'foreignField' => '_id',
                    'pipeline' => [
                        [
                            '$project' => [
                                'company_name' => 1, 'website' => 1
                            ]
                        ]
                    ],
                    'as' => 'quotes_contractor_details',
                ]
            ],
            [
                // [Quotes s3] : do $addFields here to filter out more fields to remove later below on the projection.
                '$addFields' => [
                    'quotes' => [
                        '$map' => [
                            'input' => '$quotes_details',
                            'as' => 'ed',
                            'in' => [
                                '$mergeObjects' => [
                                    '$$ed',
                                    [
                                        'contractor' => [       // Add contractor details as embedded data.  (Note: Alternatively, remove this to actually do a merge instead of embedding)
                                            '$first' => [
                                                '$filter' => [
                                                    'input' => '$quotes_contractor_details',
                                                    'cond' => [
                                                        '$eq' => ['$$this._id', '$$ed.contractor_id']
                                                    ]
                                                ]
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ],
            [
                // [Invoice s1] : convert contractor_id string to objectId
                '$addFields' => [
                    'invoices_details' => [
                        '$map' => [
                            'input' => '$invoices',
                            'in' => [
                                '$mergeObjects' => [
                                    '$$this',
                                    [
                                        'contractor_id' => MongoDbNative::_convertToNullOnError('$$this.contractor_id', 'objectId')
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ],
            [
                // [Invoices s2] : lookup contractors based on [invoices_details] converted contractor_id above.
                '$lookup' => [
                    'from' => 'contractors',
                    'localField' => 'invoices_details.contractor_id',
                    'foreignField' => '_id',
                    'pipeline' => [
                        [
                            '$project' => [
                                'company_name' => 1, 'website' => 1
                            ]
                        ]
                    ],
                    'as' => 'invoices_contractor_details',
                ]
            ],
            [
                // [Quotes s3] : do $addFields here to filter out more fields to remove later below on the projection.
                '$addFields' => [
                    'invoices' => [
                        '$map' => [
                            'input' => '$invoices_details',
                            'as' => 'ed',
                            'in' => [
                                '$mergeObjects' => [
                                    '$$ed',
                                    [
                                        'contractor' => [       // Add contractor details as embedded data.  (Note: Alternatively, remove this to actually do a merge instead of embedding)
                                            '$first' => [
                                                '$filter' => [
                                                    'input' => '$invoices_contractor_details',
                                                    'cond' => [
                                                        '$eq' => ['$$this._id', '$$ed.contractor_id']
                                                    ]
                                                ]
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ],
            [
                // [Inventory Usages s1] : convert inventory_id string to objectId
                '$addFields' => [
                    'case_inventory_usage' => [
                        '$map' => [
                            'input' => '$inventory_usages',
                            'in' => [
                                '$mergeObjects' => [
                                    '$$this',
                                    [
                                        'inventory_id' => MongoDbNative::_convertToNullOnError('$$this.inventory_id', 'objectId')
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ],
            [
                // [Inventory Usages s2] : lookup inventory based on [inventory_usage_details] converted contractor_id above.
                '$lookup' => [
                    'from' => 'inventories',
                    'localField' => 'case_inventory_usage.inventory_id',
                    'foreignField' => '_id',
                    'pipeline' => [
                        [
                            '$project' => [
                                'item' => 1, 'location' => 1, 'description' => 1, 'owner' => 1, 'serial' => 1,
                                'value' => 1, 'sell' => 1, 'quantity' => 1, 'unlimited' => 1,
                            ]
                        ]
                    ],
                    'as' => 'inventory_details',
                ]
            ],
            [
                // [Inventory Usages s3] : do $addFields here to filter out more fields to remove later below on the projection.
                '$addFields' => [
                    'inventory_usages' => [
                        '$map' => [
                            'input' => '$case_inventory_usage',
                            'as' => 'ciu',
                            'in' => [
                                '$mergeObjects' => [
                                    '$$ciu',
                                    [
                                        'inventory' => [       // Add inventory details as embedded data. (Note: Alternatively, remove this to actually do a merge instead of embedding)
                                            '$first' => [
                                                '$filter' => [
                                                    'input' => '$inventory_details',
                                                    'cond' => [
                                                        '$eq' => ['$$this._id', '$$ciu.inventory_id']
                                                    ]
                                                ]
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ],
                    [
                        '_photos' => [
                            '_id' => 1
                        ]
                    ]
                ]
            ],
            [
                '$project' => [
                    'subject' => 1,
                    'detail' => 1,
                    'history_note' => 1,
                    //'added' => 1,     //moved to created_at
                    'start' => 1,
                    'purchase_order_number' => 1,
                    'created_at' => 1,
                    'due' => 1,
                    'completion_date' => 1,
                    'number' => 1,
                    'area' => 1,

                    'apartments' => 1,

                    'inventory_usages' => 1,

                    'photos' => 1,
                    'resized_photos._id' => 1,
                    'resized_photos.file_name' => 1,
                    'resized_photos.s3key' => 1,

                    'documents' => 1,

                    'quotes._id' => 1,
                    'quotes.number' => 1,
                    'quotes.amount' => 1,
                    'quotes.fee' => 1,
                    'quotes.status' => 1,
                    'quotes.comment' => 1,
                    'quotes.contractor_id' => 1,
                    'quotes.approval_date' => 1,
                    'quotes.created_at' => 1,
                    'quotes.file._id' => 1,
                    'quotes.file.file_name' => 1,
                    'quotes.file.s3key' => 1,
                    'quotes.file.created_at' => 1,
                    'quotes.contractor' => 1,

                    'invoices._id' => 1,
                    'invoices.number' => 1,
                    'invoices.base_amount' => 1,
                    'invoices.amount' => 1,
                    'invoices.fee' => 1,
                    'invoices.status' => 1,
                    'invoices.comment' => 1,
                    'invoices.contractor_id' => 1,
                    'invoices.budget_id' => 1,
                    'invoices.approval_date' => 1,
                    'invoices.created_at' => 1,
                    'invoices.file._id' => 1,
                    'invoices.file.file_name' => 1,
                    'invoices.file.s3key' => 1,
                    'invoices.file.created_at' => 1,
                    'invoices.contractor' => 1,

                    'logs.detail' => 1,
                    'logs.full_details' => 1,
                    'logs.created_at' => 1,

                    'status' => 1,
                    'type' => 1,
                    'priority' => 1,

                    'folders' => '$folders_details',
                    'assets' => '$assets_details',

//                    'contractors_details' => 1,
                    'contractors' => 1,

                    //[Emails s3]
                    'emails' => [
                        '$map' => [
                            'input' => '$emails_details',
                            'as' => 'ed',
                            'in' => [
                                '$mergeObjects' => [
                                    '$$ed',
                                    [
                                        'contractor' => [       // Add 'contractor' details as embedded data.  (Note: Alternatively, remove this to actually do a merge instead of embedding)
                                            '$first' => [
                                                '$filter' => [
                                                    'input' => '$emails_contractor_details',
                                                    'cond' => [
                                                        '$eq' => ['$$this._id', '$$ed.contractor_id']
                                                    ]
                                                ]
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ],


                ]
            ]
        ];

        $resContacts = $mdbCasesCollection->aggregate(
            $dbFilter
        );
        $caseFullDetails = $resContacts->toArray();
        $caseFullDetails = reset($caseFullDetails);
        $s3Bucket = new AwsS3();

        if (isset($caseFullDetails['documents']) && count($caseFullDetails['documents']) > 0) {
            $documentArray = [];
            foreach ($caseFullDetails['documents'] as $doc) {
                $s3Bucket->appendSecureUrl($doc['file']);
                $doc['file']['web_secure_url'] = '/document/'.base64_encode($doc['file']['s3key']);
                $documentArray[] = $doc;
            }
            $caseFullDetails['documents'] = $documentArray;
        }

        if (isset($caseFullDetails['photos']) && count($caseFullDetails['photos']) > 0) {
            $photoArray = [];
            foreach ($caseFullDetails['photos'] as $photo) {
                $s3Bucket->appendSecureUrl($photo['file']);
                $photoArray[] = $photo;
            }
            $caseFullDetails['photos'] = $photoArray;
        }
        $caseFullDetails['history_note_format'] = str_replace("<br />", "\n", $caseFullDetails['history_note']);
        return $caseFullDetails;
    }

    /**
     * @param array $caseObjIds
     * @param Building $building
     * @return array
     */
    public static function getCasesListFullDetails(array $caseObjIds, Building $building): array
    {
        $mdbCasesCollection = app('MongoDbClient')->getCollection('cases');

        $dbFilter = [
            [
                '$match' => [
                    '_id' => [
                        '$in' => $caseObjIds
                    ],
                    'deleted_at' => null
                ]
            ],
            [
                // [Apartments]
                '$lookup' => [
                    'from' => 'apartments',
                    'localField' => 'apartments',
                    'foreignField' => '_id',
                    'pipeline' => [
                        [
                            '$match' => [
                                'deleted_at' => ['$eq' => null],
                            ]
                        ],
                        [
                            '$project' => [
                                'unit_label' => 1, 'lot' => 1, 'is_hotel' => 1, 'status' => 1,
                                '_id' => ['$toString' => '$_id']
                            ]
                        ],
                    ],
                    'as' => 'apartments',
                ]
            ],
            [
                // [Status - Categories]
                '$lookup' => [
                    'from' => 'categories',
                    'let' => [
                        'statusId' => ['$toObjectId' => '$status_id']
                    ],
                    'pipeline' => [
                        [
                            '$match' => [
                                '$expr' => [
                                    '$eq' => ['$$statusId', '$_id']
                                ]
                            ]
                        ],
                        [
                            '$project' => [
                                'name' => 1, 'type' => 1, 'management_name' => 1
                            ]
                        ]
                    ],
                    'as' => 'status'
                ]
            ],
            [
                // [Type - Categories]
                '$lookup' => [
                    'from' => 'categories',
                    'let' => [
                        'typeId' => MongoDbNative::_convertToNullOnError('$type_id', 'objectId')
                    ],
                    'pipeline' => [
                        [
                            '$match' => [
                                '$expr' => [
                                    '$eq' => ['$$typeId', '$_id']
                                ]
                            ]
                        ],
                        [
                            '$project' => [
                                'name' => 1, 'type' => 1
                            ]
                        ]
                    ],
                    'as' => 'type'
                ]
            ],
            [
                // [Priority - Categories]
                '$lookup' => [
                    'from' => 'categories',
                    'let' => [
                        'priorityId' => MongoDbNative::_convertToNullOnError('$priority_id', 'objectId')
                    ],
                    'pipeline' => [
                        [
                            '$match' => [
                                '$expr' => [
                                    '$eq' => ['$$priorityId', '$_id']
                                ]
                            ]
                        ],
                        [
                            '$project' => [
                                'name' => 1, 'type' => 1
                            ]
                        ]
                    ],
                    'as' => 'priority'
                ]
            ],
            [
                '$unwind' => [
                    'path' => '$status',
                    'preserveNullAndEmptyArrays' => true
                ]
            ],
            [
                '$unwind' => [
                    'path' => '$type',
                    'preserveNullAndEmptyArrays' => true
                ]
            ],
            [
                '$unwind' => [
                    'path' => '$priority',
                    'preserveNullAndEmptyArrays' => true
                ]
            ],
            [
                // [Contractors]
                '$lookup' => [
                    'from' => 'contractors',
                    'localField' => 'contractors.contractor_id',
                    'foreignField' => '_id',
                    'pipeline' => [
                        [
                            '$project' => [
                                'company_name' => 1, 'website' => 1, 'phone' => 1, 'email' => 1, 'contacts' => 1
                            ]
                        ]
                    ],
                    'as' => 'contractors_details',
                ]
            ],
            [
                // [Folders]
                '$lookup' => [
                    'from' => 'folders',
                    'localField' => 'folders',
                    'foreignField' => '_id',
                    'pipeline' => [
                        [
                            '$project' => [
                                'name' => 1, 'type' => 1
                            ]
                        ]
                    ],
                    'as' => 'folders_details',
                ]
            ],
            [
                // [Assets]
                '$lookup' => [
                    'from' => 'assets',
                    'localField' => 'assets',
                    'foreignField' => '_id',
                    'pipeline' => [
                        [
                            '$project' => [
                                'name' => 1, 'description' => 1, '_id' => ['$toString' => '$_id']
                            ]
                        ]
                    ],
                    'as' => 'assets_details',
                ]
            ],
            [
                // [Email s0] Sort emails by descending date.
                '$addFields' => [
                    'emails_sorted_desc_date' => [
                        '$sortArray' => [
                            'input' => '$emails',
                            'sortBy' => ['created_at' => -1]
                        ]
                    ]
                ]
            ],
            [
                // [Emails s1] : convert contractor_id string to objectId
                '$addFields' => [
                    'emails_details' => [
                        '$map' => [
                            'input' => [
                                '$slice' => [
                                    '$emails_sorted_desc_date', 100
                                ]
                            ],
                            'in' => [
                                '$mergeObjects' => [
                                    '$$this',
                                    [
                                        'contractor_id' => MongoDbNative::_convertToNullOnError('$$this.contractor_id', 'objectId')
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ],
            [
                // [Emails s2] : lookup contractors based on [emails_details] converted contractor_id above.
                '$lookup' => [
                    'from' => 'contractors',
                    'localField' => 'emails_details.contractor_id',
                    'foreignField' => '_id',
                    'pipeline' => [
                        [
                            '$project' => [
                                'company_name' => 1, 'website' => 1
                            ]
                        ]
                    ],
                    'as' => 'emails_contractor_details',
                ]
            ],
            [
                // [Quotes s1] : convert contractor_id string to objectId
                '$addFields' => [
                    'quotes_details' => [
                        '$map' => [
                            'input' => '$quotes',
                            'in' => [
                                '$mergeObjects' => [
                                    '$$this',
                                    [
                                        'contractor_id' => MongoDbNative::_convertToNullOnError('$$this.contractor_id', 'objectId')
                                    ]

                                ]
                            ]
                        ]
                    ]
                ]
            ],
            [
                // [Quotes s2] : lookup contractors based on [emails_details] converted contractor_id above.
                '$lookup' => [
                    'from' => 'contractors',
                    'localField' => 'quotes_details.contractor_id',
                    'foreignField' => '_id',
                    'pipeline' => [
                        [
                            '$project' => [
                                'company_name' => 1, 'website' => 1
                            ]
                        ]
                    ],
                    'as' => 'quotes_contractor_details',
                ]
            ],
            [
                // [Quotes s3] : do $addFields here to filter out more fields to remove later below on the projection.
                '$addFields' => [
                    'quotes' => [
                        '$map' => [
                            'input' => '$quotes_details',
                            'as' => 'ed',
                            'in' => [
                                '$mergeObjects' => [
                                    '$$ed',
                                    [
                                        'contractor' => [       // Add contractor details as embedded data.  (Note: Alternatively, remove this to actually do a merge instead of embedding)
                                            '$first' => [
                                                '$filter' => [
                                                    'input' => '$quotes_contractor_details',
                                                    'cond' => [
                                                        '$eq' => ['$$this._id', '$$ed.contractor_id']
                                                    ]
                                                ]
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ],
            [
                // [Invoice s1] : convert contractor_id string to objectId
                '$addFields' => [
                    'invoices_details' => [
                        '$map' => [
                            'input' => '$invoices',
                            'in' => [
                                '$mergeObjects' => [
                                    '$$this',
                                    [
                                        'contractor_id' => MongoDbNative::_convertToNullOnError('$$this.contractor_id', 'objectId')
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ],
            [
                // [Invoices s2] : lookup contractors based on [invoices_details] converted contractor_id above.
                '$lookup' => [
                    'from' => 'contractors',
                    'localField' => 'invoices_details.contractor_id',
                    'foreignField' => '_id',
                    'pipeline' => [
                        [
                            '$project' => [
                                'company_name' => 1, 'website' => 1
                            ]
                        ]
                    ],
                    'as' => 'invoices_contractor_details',
                ]
            ],
            [
                // [Quotes s3] : do $addFields here to filter out more fields to remove later below on the projection.
                '$addFields' => [
                    'invoices' => [
                        '$map' => [
                            'input' => '$invoices_details',
                            'as' => 'ed',
                            'in' => [
                                '$mergeObjects' => [
                                    '$$ed',
                                    [
                                        'contractor' => [       // Add contractor details as embedded data.  (Note: Alternatively, remove this to actually do a merge instead of embedding)
                                            '$first' => [
                                                '$filter' => [
                                                    'input' => '$invoices_contractor_details',
                                                    'cond' => [
                                                        '$eq' => ['$$this._id', '$$ed.contractor_id']
                                                    ]
                                                ]
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ],
            [
                // [Inventory Usages s1] : convert inventory_id string to objectId
                '$addFields' => [
                    'case_inventory_usage' => [
                        '$map' => [
                            'input' => '$inventory_usages',
                            'in' => [
                                '$mergeObjects' => [
                                    '$$this',
                                    [
                                        'inventory_id' => MongoDbNative::_convertToNullOnError('$$this.inventory_id', 'objectId')
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ],
            [
                // [Inventory Usages s2] : lookup inventory based on [inventory_usage_details] converted contractor_id above.
                '$lookup' => [
                    'from' => 'inventories',
                    'localField' => 'case_inventory_usage.inventory_id',
                    'foreignField' => '_id',
                    'pipeline' => [
                        [
                            '$project' => [
                                'item' => 1, 'location' => 1, 'description' => 1, 'owner' => 1, 'serial' => 1,
                                'value' => 1, 'sell' => 1, 'quantity' => 1, 'unlimited' => 1,
                            ]
                        ]
                    ],
                    'as' => 'inventory_details',
                ]
            ],
            [
                // [Inventory Usages s3] : do $addFields here to filter out more fields to remove later below on the projection.
                '$addFields' => [
                    'inventory_usages' => [
                        '$map' => [
                            'input' => '$case_inventory_usage',
                            'as' => 'ciu',
                            'in' => [
                                '$mergeObjects' => [
                                    '$$ciu',
                                    [
                                        'inventory' => [       // Add inventory details as embedded data. (Note: Alternatively, remove this to actually do a merge instead of embedding)
                                            '$first' => [
                                                '$filter' => [
                                                    'input' => '$inventory_details',
                                                    'cond' => [
                                                        '$eq' => ['$$this._id', '$$ciu.inventory_id']
                                                    ]
                                                ]
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ],
                    [
                        '_photos' => [
                            '_id' => 1
                        ]
                    ]
                ]
            ],
            [
                '$project' => [
                    'subject' => 1,
                    'detail' => 1,
                    'history_note' => 1,
                    'start' => 1,
                    'purchase_order_number' => 1,
                    'created_at' => 1,
                    'due' => 1,
                    'completion_date' => 1,
                    'number' => 1,
                    'area' => 1,

                    'apartments' => 1,

                    'inventory_usages' => 1,

                    'photos' => 1,
                    'resized_photos._id' => 1,
                    'resized_photos.file_name' => 1,
                    'resized_photos.s3key' => 1,

                    'documents' => 1,

                    'quotes._id' => 1,
                    'quotes.number' => 1,
                    'quotes.amount' => 1,
                    'quotes.fee' => 1,
                    'quotes.status' => 1,
                    'quotes.comment' => 1,
                    'quotes.contractor_id' => 1,
                    'quotes.approval_date' => 1,
                    'quotes.created_at' => 1,
                    'quotes.file._id' => 1,
                    'quotes.file.file_name' => 1,
                    'quotes.file.s3key' => 1,
                    'quotes.file.created_at' => 1,
                    'quotes.contractor' => 1,

                    'invoices._id' => 1,
                    'invoices.number' => 1,
                    'invoices.base_amount' => 1,
                    'invoices.amount' => 1,
                    'invoices.fee' => 1,
                    'invoices.status' => 1,
                    'invoices.comment' => 1,
                    'invoices.contractor_id' => 1,
                    'invoices.budget_id' => 1,
                    'invoices.approval_date' => 1,
                    'invoices.created_at' => 1,
                    'invoices.file._id' => 1,
                    'invoices.file.file_name' => 1,
                    'invoices.file.s3key' => 1,
                    'invoices.file.created_at' => 1,
                    'invoices.contractor' => 1,

                    'logs.detail' => 1,
                    'logs.full_details' => 1,
                    'logs.created_at' => 1,
                    'status' => 1,
                    'type' => 1,
                    'priority' => 1,
                    'folders' => '$folders_details',
                    'assets' => '$assets_details',
                    'contractors' => 1,
                    'emails' => [
                        '$map' => [
                            'input' => '$emails_details',
                            'as' => 'ed',
                            'in' => [
                                '$mergeObjects' => [
                                    '$$ed',
                                    [
                                        'contractor' => [       // Add 'contractor' details as embedded data.  (Note: Alternatively, remove this to actually do a merge instead of embedding)
                                            '$first' => [
                                                '$filter' => [
                                                    'input' => '$emails_contractor_details',
                                                    'cond' => [
                                                        '$eq' => ['$$this._id', '$$ed.contractor_id']
                                                    ]
                                                ]
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ],
                ]
            ]
        ];

        $resContacts = $mdbCasesCollection->aggregate(
            $dbFilter
        );
        return iterator_to_array($resContacts);
    }

    /**
     * @param array $caseObjIds
     * @param Building $building
     * @return array
     */
    public static function getCasesListFullDetailsV2(array $filter, Building $building): array
    {
        $mongoDb = app('MongoDbClient');
        $casesCollection = $mongoDb->getCollection('cases');

        $sortSpec = [$filter['sort_by'] => 1];
        if ($filter['sort_mode'] === 'desc') {
            $sortSpec = [$filter['sort_by'] => -1];
        }

        /** Gather initial $match filter(s). **/
        $mdbMatchStage = [
            '$or' => [
                ['building_id' => $building['_id']],
                [
                    /** Also get building synced cases **/
                    'duplicate_sync_building_ids' => [
                        '$in' => [$building['_id']]
                    ]
                ]
            ],
            'deleted_at' => null,
        ];

        if (!empty($filter['created_at_min']) && !empty($filter['created_at_max'])) {
            $create_at_min = MybosTime::CarbonToUTCDateTime(Carbon::createFromFormat('Y-m-d H:i:s', $filter['created_at_min'] . ' 00:00:00', $building['timezone']));
            $create_at_max = MybosTime::CarbonToUTCDateTime(Carbon::createFromFormat('Y-m-d H:i:s', $filter['created_at_max'] . ' 23:59:59', $building['timezone']));
            $mdbMatchStage['$and'] = [
                ['created_at' => ['$gte' => $create_at_min]],
                ['created_at' => ['$lte' => $create_at_max]]
            ];
        }

        if (!empty($filter['updated_at_min']) && !empty($filter['updated_at_max'])) {
            $update_at_min = MybosTime::CarbonToUTCDateTime(Carbon::createFromFormat('Y-m-d H:i:s', $filter['updated_at_min'] . ' 00:00:00', $building['timezone']));
            $update_at_max = MybosTime::CarbonToUTCDateTime(Carbon::createFromFormat('Y-m-d H:i:s', $filter['updated_at_max'] . ' 23:59:59', $building['timezone']));
            $mdbMatchStage['$and'] = [
                ['updated_at' => ['$gte' => $update_at_min]],
                ['updated_at' => ['$lte' => $update_at_max]]
            ];
        }

        $dbFilter = [
            [
                '$match' => $mdbMatchStage
            ],
            [
                // [Apartments]
                '$lookup' => [
                    'from' => 'apartments',
                    'localField' => 'apartments',
                    'foreignField' => '_id',
                    'pipeline' => [
                        [
                            '$match' => [
                                'deleted_at' => ['$eq' => null],
                            ]
                        ],
                        [
                            '$project' => [
                                'unit_label' => 1, 'lot' => 1, 'is_hotel' => 1, 'status' => 1,
                                '_id' => ['$toString' => '$_id']
                            ]
                        ],
                    ],
                    'as' => 'apartments',
                ]
            ],
            [
                // [Status - Categories]
                '$lookup' => [
                    'from' => 'categories',
                    'let' => [
                        'statusId' => ['$toObjectId' => '$status_id']
                    ],
                    'pipeline' => [
                        [
                            '$match' => [
                                '$expr' => [
                                    '$eq' => ['$$statusId', '$_id']
                                ]
                            ]
                        ],
                        [
                            '$project' => [
                                'name' => 1, 'type' => 1, 'management_name' => 1
                            ]
                        ]
                    ],
                    'as' => 'status'
                ]
            ],
            [
                // [Type - Categories]
                '$lookup' => [
                    'from' => 'categories',
                    'let' => [
                        'typeId' => MongoDbNative::_convertToNullOnError('$type_id', 'objectId')
                    ],
                    'pipeline' => [
                        [
                            '$match' => [
                                '$expr' => [
                                    '$eq' => ['$$typeId', '$_id']
                                ]
                            ]
                        ],
                        [
                            '$project' => [
                                'name' => 1, 'type' => 1
                            ]
                        ]
                    ],
                    'as' => 'type'
                ]
            ],
            [
                // [Priority - Categories]
                '$lookup' => [
                    'from' => 'categories',
                    'let' => [
                        'priorityId' => MongoDbNative::_convertToNullOnError('$priority_id', 'objectId')
                    ],
                    'pipeline' => [
                        [
                            '$match' => [
                                '$expr' => [
                                    '$eq' => ['$$priorityId', '$_id']
                                ]
                            ]
                        ],
                        [
                            '$project' => [
                                'name' => 1, 'type' => 1
                            ]
                        ]
                    ],
                    'as' => 'priority'
                ]
            ],
            [
                '$unwind' => [
                    'path' => '$status',
                    'preserveNullAndEmptyArrays' => true
                ]
            ],
            [
                '$unwind' => [
                    'path' => '$type',
                    'preserveNullAndEmptyArrays' => true
                ]
            ],
            [
                '$unwind' => [
                    'path' => '$priority',
                    'preserveNullAndEmptyArrays' => true
                ]
            ],
            [
                // [Contractors]
                '$lookup' => [
                    'from' => 'contractors',
                    'localField' => 'contractors.contractor_id',
                    'foreignField' => '_id',
                    'pipeline' => [
                        [
                            '$project' => [
                                'company_name' => 1, 'website' => 1, 'phone' => 1, 'email' => 1, 'contacts' => 1
                            ]
                        ]
                    ],
                    'as' => 'contractors_details',
                ]
            ],
            [
                // [Folders]
                '$lookup' => [
                    'from' => 'folders',
                    'localField' => 'folders',
                    'foreignField' => '_id',
                    'pipeline' => [
                        [
                            '$project' => [
                                'name' => 1, 'type' => 1
                            ]
                        ]
                    ],
                    'as' => 'folders_details',
                ]
            ],
            [
                // [Assets]
                '$lookup' => [
                    'from' => 'assets',
                    'localField' => 'assets',
                    'foreignField' => '_id',
                    'pipeline' => [
                        [
                            '$project' => [
                                'name' => 1, 'description' => 1, '_id' => ['$toString' => '$_id']
                            ]
                        ]
                    ],
                    'as' => 'assets_details',
                ]
            ],
            [
                // [Email s0] Sort emails by descending date.
                '$addFields' => [
                    'emails_sorted_desc_date' => [
                        '$sortArray' => [
                            'input' => '$emails',
                            'sortBy' => ['created_at' => -1]
                        ]
                    ]
                ]
            ],
            [
                // [Emails s1] : convert contractor_id string to objectId
                '$addFields' => [
                    'emails_details' => [
                        '$map' => [
                            'input' => [
                                '$slice' => [
                                    '$emails_sorted_desc_date', 100
                                ]
                            ],
                            'in' => [
                                '$mergeObjects' => [
                                    '$$this',
                                    [
                                        'contractor_id' => MongoDbNative::_convertToNullOnError('$$this.contractor_id', 'objectId')
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ],
            [
                // [Emails s2] : lookup contractors based on [emails_details] converted contractor_id above.
                '$lookup' => [
                    'from' => 'contractors',
                    'localField' => 'emails_details.contractor_id',
                    'foreignField' => '_id',
                    'pipeline' => [
                        [
                            '$project' => [
                                'company_name' => 1, 'website' => 1
                            ]
                        ]
                    ],
                    'as' => 'emails_contractor_details',
                ]
            ],
            [
                // [Quotes s1] : convert contractor_id string to objectId
                '$addFields' => [
                    'quotes_details' => [
                        '$map' => [
                            'input' => '$quotes',
                            'in' => [
                                '$mergeObjects' => [
                                    '$$this',
                                    [
                                        'contractor_id' => MongoDbNative::_convertToNullOnError('$$this.contractor_id', 'objectId')
                                    ]

                                ]
                            ]
                        ]
                    ]
                ]
            ],
            [
                // [Quotes s2] : lookup contractors based on [emails_details] converted contractor_id above.
                '$lookup' => [
                    'from' => 'contractors',
                    'localField' => 'quotes_details.contractor_id',
                    'foreignField' => '_id',
                    'pipeline' => [
                        [
                            '$project' => [
                                'company_name' => 1, 'website' => 1
                            ]
                        ]
                    ],
                    'as' => 'quotes_contractor_details',
                ]
            ],
            [
                // [Quotes s3] : do $addFields here to filter out more fields to remove later below on the projection.
                '$addFields' => [
                    'quotes' => [
                        '$map' => [
                            'input' => '$quotes_details',
                            'as' => 'ed',
                            'in' => [
                                '$mergeObjects' => [
                                    '$$ed',
                                    [
                                        'contractor' => [       // Add contractor details as embedded data.  (Note: Alternatively, remove this to actually do a merge instead of embedding)
                                            '$first' => [
                                                '$filter' => [
                                                    'input' => '$quotes_contractor_details',
                                                    'cond' => [
                                                        '$eq' => ['$$this._id', '$$ed.contractor_id']
                                                    ]
                                                ]
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ],
            [
                // [Invoice s1] : convert contractor_id string to objectId
                '$addFields' => [
                    'invoices_details' => [
                        '$map' => [
                            'input' => '$invoices',
                            'in' => [
                                '$mergeObjects' => [
                                    '$$this',
                                    [
                                        'contractor_id' => MongoDbNative::_convertToNullOnError('$$this.contractor_id', 'objectId')
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ],
            [
                // [Invoices s2] : lookup contractors based on [invoices_details] converted contractor_id above.
                '$lookup' => [
                    'from' => 'contractors',
                    'localField' => 'invoices_details.contractor_id',
                    'foreignField' => '_id',
                    'pipeline' => [
                        [
                            '$project' => [
                                'company_name' => 1, 'website' => 1
                            ]
                        ]
                    ],
                    'as' => 'invoices_contractor_details',
                ]
            ],
            [
                // [Quotes s3] : do $addFields here to filter out more fields to remove later below on the projection.
                '$addFields' => [
                    'invoices' => [
                        '$map' => [
                            'input' => '$invoices_details',
                            'as' => 'ed',
                            'in' => [
                                '$mergeObjects' => [
                                    '$$ed',
                                    [
                                        'contractor' => [       // Add contractor details as embedded data.  (Note: Alternatively, remove this to actually do a merge instead of embedding)
                                            '$first' => [
                                                '$filter' => [
                                                    'input' => '$invoices_contractor_details',
                                                    'cond' => [
                                                        '$eq' => ['$$this._id', '$$ed.contractor_id']
                                                    ]
                                                ]
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ],
            [
                // [Inventory Usages s1] : convert inventory_id string to objectId
                '$addFields' => [
                    'case_inventory_usage' => [
                        '$map' => [
                            'input' => '$inventory_usages',
                            'in' => [
                                '$mergeObjects' => [
                                    '$$this',
                                    [
                                        'inventory_id' => MongoDbNative::_convertToNullOnError('$$this.inventory_id', 'objectId')
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ],
            [
                // [Inventory Usages s2] : lookup inventory based on [inventory_usage_details] converted contractor_id above.
                '$lookup' => [
                    'from' => 'inventories',
                    'localField' => 'case_inventory_usage.inventory_id',
                    'foreignField' => '_id',
                    'pipeline' => [
                        [
                            '$project' => [
                                'item' => 1, 'location' => 1, 'description' => 1, 'owner' => 1, 'serial' => 1,
                                'value' => 1, 'sell' => 1, 'quantity' => 1, 'unlimited' => 1,
                            ]
                        ]
                    ],
                    'as' => 'inventory_details',
                ]
            ],
            [
                // [Inventory Usages s3] : do $addFields here to filter out more fields to remove later below on the projection.
                '$addFields' => [
                    'inventory_usages' => [
                        '$map' => [
                            'input' => '$case_inventory_usage',
                            'as' => 'ciu',
                            'in' => [
                                '$mergeObjects' => [
                                    '$$ciu',
                                    [
                                        'inventory' => [       // Add inventory details as embedded data. (Note: Alternatively, remove this to actually do a merge instead of embedding)
                                            '$first' => [
                                                '$filter' => [
                                                    'input' => '$inventory_details',
                                                    'cond' => [
                                                        '$eq' => ['$$this._id', '$$ciu.inventory_id']
                                                    ]
                                                ]
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ],
                    [
                        '_photos' => [
                            '_id' => 1
                        ]
                    ]
                ]
            ],
            [
                '$project' => [
                    'subject' => 1,
                    'detail' => 1,
                    'history_note' => 1,
                    'start' => 1,
                    'purchase_order_number' => 1,
                    'created_at' => 1,
                    'due' => 1,
                    'completion_date' => 1,
                    'number' => 1,
                    'area' => 1,

                    'apartments' => 1,

                    'inventory_usages' => 1,

                    'photos' => 1,
                    'resized_photos._id' => 1,
                    'resized_photos.file_name' => 1,
                    'resized_photos.s3key' => 1,

                    'documents' => 1,

                    'quotes._id' => 1,
                    'quotes.number' => 1,
                    'quotes.amount' => 1,
                    'quotes.fee' => 1,
                    'quotes.status' => 1,
                    'quotes.comment' => 1,
                    'quotes.contractor_id' => 1,
                    'quotes.approval_date' => 1,
                    'quotes.created_at' => 1,
                    'quotes.file._id' => 1,
                    'quotes.file.file_name' => 1,
                    'quotes.file.s3key' => 1,
                    'quotes.file.created_at' => 1,
                    'quotes.contractor' => 1,

                    'invoices._id' => 1,
                    'invoices.number' => 1,
                    'invoices.base_amount' => 1,
                    'invoices.amount' => 1,
                    'invoices.fee' => 1,
                    'invoices.status' => 1,
                    'invoices.comment' => 1,
                    'invoices.contractor_id' => 1,
                    'invoices.budget_id' => 1,
                    'invoices.approval_date' => 1,
                    'invoices.created_at' => 1,
                    'invoices.file._id' => 1,
                    'invoices.file.file_name' => 1,
                    'invoices.file.s3key' => 1,
                    'invoices.file.created_at' => 1,
                    'invoices.contractor' => 1,

                    'logs.detail' => 1,
                    'logs.full_details' => 1,
                    'logs.created_at' => 1,
                    'status' => 1,
                    'type' => 1,
                    'priority' => 1,
                    'folders' => '$folders_details',
                    'assets' => '$assets_details',
                    'contractors' => 1,
                    'emails' => [
                        '$map' => [
                            'input' => '$emails_details',
                            'as' => 'ed',
                            'in' => [
                                '$mergeObjects' => [
                                    '$$ed',
                                    [
                                        'contractor' => [       // Add 'contractor' details as embedded data.  (Note: Alternatively, remove this to actually do a merge instead of embedding)
                                            '$first' => [
                                                '$filter' => [
                                                    'input' => '$emails_contractor_details',
                                                    'cond' => [
                                                        '$eq' => ['$$this._id', '$$ed.contractor_id']
                                                    ]
                                                ]
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ],
                ]
            ]
        ];

        // Return paginated [cases] data.
        return $mongoDb->paginatedAggregationSearch(
            ['pageSize' => $filter['limit'], 'pageNumber' => $filter['page'], 'sort' => $sortSpec],
            $casesCollection, $dbFilter
        );
    }


    /**
     * @param array $caseContractorData
     * @return array
     */
    public static function getDetailedContractors(array $caseContractorData): array
    {
        $mdbContractors = app('MongoDbClient')->getCollection('contractors');
        $detailedCaseContractors = [];

        // Pluck contractor IDs
        $contractorIds = array_map(function ($eachItem) {
            if (is_string($eachItem['contractor_id'])) {
                // Use BSON object id to query contractors down later.
                return new ObjectId($eachItem['contractor_id']);
            }
            return $eachItem['contractor_id'];
        }, $caseContractorData);

        // Get contractors with more details from db.
        $detailedContractors = $mdbContractors->find(
            [
                '_id' => ['$in' => $contractorIds],
            ],
            [
                'projection' => [
                    'company_name' => 1, 'email' => 1, 'phone' => 1, 'status' => 1, 'contacts' => 1,
                    'address' => 1, 'website' => 1, 'abn' => 1, 'fax' => 1, 'notes' => 1
                ],
            ]
        );

        // Filter only the selected contractor contacts
        foreach ($detailedContractors as $detailedContractor) {

            // Get matching contractor from case's contractor.
            $caseContractor = array_filter($caseContractorData, function ($eachItem) use ($detailedContractor) {
                return $eachItem['contractor_id'] == $detailedContractor['_id'];
            });
            $caseContractor = count($caseContractor) > 0 ? reset($caseContractor) : $caseContractor;    // Expecting only one result so getting rid of key here...

            // Filter out not selected contacts from contractor company.
            $filteredContacts = collect($detailedContractor['contacts'])->filter(function ($eachContact) use ($caseContractor) {
                return in_array($eachContact['uid'], (array)$caseContractor['contacts']);
            });

            // Reset index, as filtered in values will still carry over their indexes.
            $clearedIndexFilteredContacts = array_values($filteredContacts->toArray());

            // Update contacts
            $detailedContractor['contacts'] = $clearedIndexFilteredContacts;
            $detailedCaseContractors[] = $detailedContractor;
        }

        return $detailedCaseContractors;
    }

    /**
     * @param array $caseContractorData
     * @param Building|null $building
     * @return array
     */
    public static function getDetailedContractorsWithDocuments(array $caseContractorData, Building $building = null): array
    {
        $detailedCaseContractors = [];

        // Return empty set if no contractor parameter is empty.
        if (empty($caseContractorData)) {
            return $detailedCaseContractors;
        }

        // Pluck contractor IDs
        $contractorIds = array_map(function ($eachItem) {
            if (is_string($eachItem['contractor_id'])) {
                // Use BSON object id to query contractors down later.
                return new ObjectId($eachItem['contractor_id']);
            }
            return $eachItem['contractor_id'];
        }, $caseContractorData);

        // Get contractors with more details from db.
        $detailedContractors = GlobalContractorRepository::CompaniesList(
            ['mode' => 'by building', 'contractor_ids' => $contractorIds, 'status' => 'ALL'],
            ['page' => 1, 'limit' => 100],
            $building
        );

        // Filter only the selected contractor contacts
        foreach ($detailedContractors['data'] as $detailedContractor) {

            // Get matching contractor from case's contractor.
            $caseContractor = array_filter($caseContractorData, function ($eachItem) use ($detailedContractor) {
                return $eachItem['contractor_id'] == $detailedContractor['_id'];
            });
            $caseContractor = count($caseContractor) > 0 ? reset($caseContractor) : $caseContractor;    // Expecting only one result so getting rid of key here...

            // Filter out not selected contacts from contractor company.
            $filteredContacts = collect($detailedContractor['contacts'])->filter(function ($eachContact) use ($caseContractor) {
                return in_array($eachContact['uid'], (array)($caseContractor['contacts'] ?? []));
            });

            // Reset index, as filtered in values will still carry over their indexes.
            $clearedIndexFilteredContacts = array_values($filteredContacts->toArray());

            // Update contacts
            $detailedContractor['contacts'] = $clearedIndexFilteredContacts;
            $detailedCaseContractors[] = $detailedContractor;
        }

        return $detailedCaseContractors;
    }


    /**
     * @param array $newCaseInventoryUsage
     * @param string $case_id
     * @return mixed
     */
    public static function addInventoryUsageMdb(array $newCaseInventoryUsage, string $case_id): mixed
    {
        $collection = app('MongoDbClient')->getCollection('cases');
        $newCaseInventoryUsageData = new CaseInventoryUsageData($newCaseInventoryUsage);
        $newCaseInventoryUsageModel = new CaseInventoryUsage($newCaseInventoryUsage);

        if ($newCaseInventoryUsageData->validateDataModelProperties($newCaseInventoryUsageModel)) {
            $updateOneResponse = $collection->updateOne(
                [
                    '_id' => new ObjectId($case_id)
                ],
                [
                    '$push' => ['inventory_usages' => $newCaseInventoryUsage]
                ]
            );
            return $updateOneResponse->getModifiedCount();
        }

        return false;
    }

    /**
     * @param array $inventoryUsage
     * @param string $case_id
     * @return mixed
     */
    public static function updateInventoryUsageQuantityMdb(array $inventoryUsage, string $case_id): mixed
    {
        if (!empty($inventoryUsage['quantity'])) {
            $collection = app('MongoDbClient')->getCollection('cases');
            $updateOneResponse = $collection->updateOne(
                [
                    '_id' => new ObjectId($case_id),
                    'inventory_usages' => [
                        '$elemMatch' => ['inventory_id' => $inventoryUsage['inventory_id']]
                    ]
                ],
                [
                    '$set' => ['inventory_usages.$.quantity' => (int)$inventoryUsage['quantity']]
                ]
            );

            return $updateOneResponse->getModifiedCount();

        } else {
            $collection = app('MongoDbClient')->getCollection('cases');
            $updateOneResponse = $collection->updateOne(
                [
                    '_id' => new ObjectId($case_id),
                ],
                [
                    '$pull' => [
                        'inventory_usages' => [
                            'inventory_id' => $inventoryUsage['inventory_id']
                        ]
                    ]
                ]
            );

            return $updateOneResponse->getModifiedCount();
        }
    }

    public static function CaseDelete($inputData, Building $building)
    {
        foreach ($inputData['_ids'] as $_id) {
            Cases::where('_id', $_id)
                ->where('building_id', $building['_id'])
                ->delete();
        }
        return true;
    }

    public static function SumTotalBudgetValue($budget_id)
    {
        $mongoDb = app('MongoDbClient');
        $collection = $mongoDb->getCollection('cases');
        $result = $collection->aggregate(
            [
                [
                    '$unwind' => '$invoices'
                ],
                [
                    '$match' => [
                        'invoices.budget_id' => [
                            '$eq' => $budget_id
                        ]
                    ]
                ],
                [
                    '$addFields' => [
                        'amount' => '$invoices.amount'
                    ]
                ],
                [
                    '$project' => [
                        'invoices' => 0,
                    ]
                ]
            ]
        );
        $result = $result->toArray();
        $total = 0;
        if (!empty($result)) {
            foreach ($result as $row) {
                $total = $total + $row['amount'];
            }
        }
        return $total;
    }

    public static function shortList($inputData, $building)
    {

        $mongoDb = app('MongoDbClient');
        $casesCollection = $mongoDb->getCollection('cases');
        $limit = 30;
        $page = 1;

        $sortSpec = ['_id' => 1];
        $mdbStage1Pipeline = [
            'deleted_at' => null,
            'building_id' => $building->_id
        ];

        // Assemble query pipelines.
        $mdbPipeline = [
            ['$match' => $mdbStage1Pipeline]
        ];

        if (isset($inputData['keyword']) && !empty($inputData['keyword'])) {
            // If search key is not empty.
            if (trim($inputData['keyword']) != '') {
                $mdbStageSearchPipeline = [];
                $filterKeyword = trim($inputData['keyword']);
                $mdbStageSearchPipeline['$match'] = [
                    '$or' => [
                        ['area' => ['$regex' => $filterKeyword, '$options' => 'i']],
                        ['subject' => ['$regex' => $filterKeyword, '$options' => 'i']],
                        ['$expr' => [
                            '$regexMatch' => [
                                'input' => ['$toString' => '$number'],
                                'regex' => $filterKeyword
                            ]
                        ]
                        ],
                        ['priority' => ['$regex' => $filterKeyword, '$options' => 'i']],
                    ]
                ];
                $mdbPipeline[] = $mdbStageSearchPipeline;
            }
        }

        $mdbStage2Pipeline = [
            '$addFields' => [
                '_id_str' => [
                    // Get string converted _id.
                    '$toString' => '$_id'
                ],
            ]
        ];

        $mdbStage3Pipeline = [
            '$project' => [
                '_id' => 1,
                '_id_str' => 1,
                'number' => 1,
                'subject' => 1,
            ]
        ];

        array_push($mdbPipeline, $mdbStage2Pipeline, $mdbStage3Pipeline);

        $queryResult = $mongoDb->paginatedAggregationSearch(
            ['pageSize' => $limit, 'pageNumber' => $page, 'sort' => $sortSpec],
            $casesCollection, $mdbPipeline
        );
        return $queryResult;
    }

    private static function _formatEmail($email_template, $ContactName, $buildingName, $number): array
    {
        $phrase = $email_template['body'] ?? null;
        $phraseSubject = $email_template['subject'] ?? null;
        $tags = ["{ContactName}", "{BuildingName}", "{Number}",];
        $values = [
            $ContactName,
            $buildingName,
            $number,
        ];
        $newPhrase = str_replace($tags, $values, $phrase);
        $newPhraseSubject = str_replace($tags, $values, $phraseSubject);

        $rv = [
            'body' => $newPhrase,
            'subject' => $newPhraseSubject,
        ];
        return $rv;

    }

    /**
     * @param Cases $case
     * @param Building $building
     * @return bool
     */
    public static function sendCaseUpdateNotification(Cases $case, Building $building): bool
    {
        $mybosMessagingTemplateResident = $building->getEmailTemplateSettingData(MybosMessaging::MAINTENANCE_REQUEST, MybosMessaging::_MAINTENANCE_REQUEST_STATUS_NOTIFICATION);
        if ($mybosMessagingTemplateResident instanceof EmailTemplateSettingsData && $mybosMessagingTemplateResident->status) {
            // Process some email data.
            $formattedDateCreated = Carbon::instance($case->maintenanceRequest['created_at'])->timezone($building['timezone'])->format(MybosDateTimeFormat::AUSTRALIA_DATE);
            $maintenanceTypeCategory = Category::find($case->maintenanceRequest['category_id']);
            $caseStatusCategory = Category::find($case['status_id']);

            // Assemble the placeholder replacement values.
            $placeHolderReplacements = [
                "{Name}" => $case->maintenanceRequest['first_name'] ?? '',
                "{CaseNumber}" => $case['number'],
                "{Number}" => $case['number'],
                "{CaseStatus}" => $caseStatusCategory['name'] ?? '',
                "{Status}" => $caseStatusCategory['name'] ?? '',
                "{Apartment}" => $case->maintenanceRequest->apartment_details['unit_label'] ?? '',
                "{Building}" => $building['name'] ?? '',
                "{FirstName}" => $case->maintenanceRequest['first_name'] ?? '',
                "{LastName}" => $case->maintenanceRequest['last_name'] ?? '',
                "{Email}" => $case->maintenanceRequest['email'] ?? '',
                "{Mobile}" => $case->maintenanceRequest['mobile'] ?? '',
                "{Phone}" => $case->maintenanceRequest['phone'] ?? '',
                "{Date}" => $formattedDateCreated,
                "{Type}" => $maintenanceTypeCategory['name'] ?? '',
                "{Description}" => $case->maintenanceRequest['details'] ?? '',
            ];

            // Send email receipt of this request to the requesting-resident
            if (MybosString::isValidEmail($case->maintenanceRequest['email']) && Helpers2::canEnvSendToEmail($case->maintenanceRequest['email'])) {
                $mailableStandardTemplatedEmail = new StandardTemplatedEmail($placeHolderReplacements, $mybosMessagingTemplateResident, $building);
                Mail::to([
                    ['email' => $case->maintenanceRequest['email'], 'name' => $case->maintenanceRequest['first_name'] . $case->maintenanceRequest['last_name']],
                ])->queue($mailableStandardTemplatedEmail);
            }
        }
        return true;
    }
}
