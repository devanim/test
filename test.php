<?php
namespace App\Jobs;

use App\Models\CheckingAccount;
use App\Models\CreditCard;
use App\Models\Customer;
use App\Models\RiskIgnoreList;
use App\Models\Config;
use App\Models\RiskyTransaction;
use App\Models\RiskyTransactionDetail;
use App\Models\Transaction;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;

class DailyRiskManager implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;
    protected $startTime;
    protected $endTime;
    protected $existingRisk;
    protected $newRisk;
    public function __construct()
    {
        $this->startTime = now()->subDays(30)->startOfDay();
        $this->endTime = now()->endOfDay();
    }

    public function handle()
    {
        // Thresholds fetch & constants
        $thresholds = DB::table('risky_parameters')->pluck('threshold', 'risk_parameter_id')->toArray();
        $constants = config('risk.constants');

        // Ignore list fetch
        $ignoreList = RiskIgnoreList::select('item', 'type')->get();
        $groupedIgnoreList = $ignoreList->groupBy('type');

        $testCards = $groupedIgnoreList->get('CARD', collect())->pluck('item')->toArray();
        $testChecks = $groupedIgnoreList->get('CHECK', collect())->pluck('item')->toArray();
        $approvedIps = $groupedIgnoreList->get('IP', collect())->pluck('item')->toArray();


        $merchantExceptions = Config::whereNotNull('test_merchant')->where('test_merchant', '<>', '0')->pluck('test_merchant')->prepend("EZPAYEZ")->toArray();
        
        // Existing Risk
        $existingRisk = RiskyTransaction::leftJoin('risky_transaction_detail as rtd', 'rtd.risky_transaction_id', '=', 'risky_transactions.risky_transaction_id')
            ->select('risky_transactions.risk_item', 'risky_transactions.risk_type', 'rtd.param', 'rtd.value', 'risky_transactions.risky_transaction_id')
            ->get()->toArray();
        $existingRisk = $existingRisk ?: [];
        
        // Query for rfCard
        $query = Transaction::join('merchants as m', 'm.merchant_id', '=', 'transactions.merchant_id')
            ->join('rooms as r', 'r.room_id', '=', 'transactions.room_id')
            ->join('creditcards as cc', 'cc.cc_transaction_id', '=', 'transactions.transaction_id')
            ->select('cc.cc_number', DB::raw('COUNT(*) AS count'), DB::raw('SUM(transactions.amount) AS amount'))
            ->where([
                ['transactions.status', '=', 'REFUNDED'],
                ['transactions.date', '>=', $this->startTime],
                ['transactions.date', '<=', $this->endTime],
                ['transactions.test_mode', '=', 'L'],
                ['r.three_d', '=', 0]
            ]);
            
    
        if (!empty($testCards)) {
            $query = $query->whereNotIn('cc.cc_number', $testCards);
        }
        
        if (!empty($merchantExceptions)) {
            $query = $query->whereNotIn('m.merchant_name', $merchantExceptions);
        }
        
        
        $rfCard = $query->groupBy('cc.cc_number')
            ->havingRaw("COUNT(*) > ?", [$thresholds[$constants['MRC']]])
            ->get();
        
        // Query for rfCheck
        $query = Transaction::join('merchants as m', 'm.merchant_id', '=', 'transactions.merchant_id')
            ->join('checking_accounts as ca', 'ca.transaction_id', '=', 'transactions.transaction_id')
            ->select('ca.account_number', DB::raw('COUNT(*) AS count'))
            ->where([
                ['transactions.status', '=', 'REFUNDED'],
                ['transactions.date', '>=', $this->startTime],
                ['transactions.date', '<=', $this->endTime],
                ['transactions.test_mode', '=', 'L']
            ]);
            
    
        if (!empty($testChecks)) {
            $query = $query->whereNotIn('ca.account_number', $testChecks);
        }
        
        if (!empty($merchantExceptions)) {
            $query = $query->whereNotIn('m.merchant_name', $merchantExceptions);
        }
        
        $rfCheck = $query->groupBy('ca.account_number')
            ->havingRaw("COUNT(*) > ?", [$thresholds[$constants['MRC']]])
            ->get();

        // Query for rtCheck
        $query = Transaction::join('merchants as m', 'm.merchant_id', '=', 'transactions.merchant_id')
            ->join('checking_accounts as ca', 'ca.transaction_id', '=', 'transactions.transaction_id')
            ->select('ca.account_number', DB::raw('COUNT(*) AS count'))
            ->where([
                ['transactions.status', '=', 'RETURNED'],
                ['transactions.date', '>=', $this->startTime],
                ['transactions.date', '<=', $this->endTime],
                ['transactions.test_mode', '=', 'L']
            ]);
            
    
        if (!empty($testChecks)) {
            $query = $query->whereNotIn('ca.account_number', $testChecks);
        }
        
        if (!empty($merchantExceptions)) {
            $query = $query->whereNotIn('m.merchant_name', $merchantExceptions);
        }
        
        $rtCheck = $query->groupBy('ca.account_number')
            ->havingRaw("COUNT(*) > ?", [$thresholds[$constants['MTC']]])
            ->get();

        // Query for cbCard
        $query = Transaction::join('merchants as m', 'm.merchant_id', '=', 'transactions.merchant_id')
            ->join('rooms as r', 'r.room_id', '=', 'transactions.room_id')
            ->join('creditcards as cc', 'cc.cc_transaction_id', '=', 'transactions.transaction_id')
            ->select('cc.cc_number', DB::raw('COUNT(*) AS count'))
            ->where([
                ['transactions.status', '=', 'CHARGEDBACK'],
                ['transactions.date', '>=', $this->startTime],
                ['transactions.date', '<=', $this->endTime],
                ['transactions.test_mode', '=', 'L']
            ]);
            

        if (!empty($testCards)) {
            $query = $query->whereNotIn('cc.cc_number', $testCards);
        }

        if (!empty($merchantExceptions)) {
            $query = $query->whereNotIn('m.merchant_name', $merchantExceptions);
        }

        $cbCard = $query->groupBy('cc.cc_number')
            ->havingRaw("COUNT(*) > ?", [$thresholds[$constants['MCC']]])
            ->get();
        
        // Query for cbCheck
        $query = Transaction::join('merchants as m', 'm.merchant_id', '=', 'transactions.merchant_id')
            ->join('checking_accounts as ca', 'ca.transaction_id', '=', 'transactions.transaction_id')
            ->select('ca.account_number', DB::raw('COUNT(*) AS count'))
            ->where([
                ['transactions.status', '=', 'CHARGEDBACK'],
                ['transactions.date', '>=', $this->startTime],
                ['transactions.date', '<=', $this->endTime],
                ['transactions.test_mode', '=', 'L']
            ]);
            
        
        if (!empty($testChecks)) {
            $query = $query->whereNotIn('ca.account_number', $testChecks);
        }
        
        if (!empty($merchantExceptions)) {
            $query = $query->whereNotIn('m.merchant_name', $merchantExceptions);
        }
        
        $cbCheck = $query->groupBy('ca.account_number')
            ->havingRaw("COUNT(*) > ?", [$thresholds[$constants['MCC']]])
            ->get();

        // Query for deCard
        $query = Transaction::join('merchants as m', 'm.merchant_id', '=', 'transactions.merchant_id')
            ->join('rooms as r', 'r.room_id', '=', 'transactions.room_id')
            ->join('creditcards as cc', 'cc.cc_transaction_id', '=', 'transactions.transaction_id')
            ->select('cc.cc_number', DB::raw('COUNT(*) AS count'))
            ->where([
                ['transactions.status', '=', 'DECLINED'],
                ['transactions.date', '>=', $this->startTime],
                ['transactions.date', '<=', $this->endTime],
                ['transactions.test_mode', '=', 'L'],
                ['r.three_d', '=', 0]
            ]);
            

        if (!empty($testCards)) {
            $query = $query->whereNotIn('cc.cc_number', $testCards);
        }

        if (!empty($merchantExceptions)) {
            $query = $query->whereNotIn('m.merchant_name', $merchantExceptions);
        }

        $deCard = $query->groupBy('cc.cc_number')
            ->havingRaw("COUNT(*) > ?", [$thresholds[$constants['MDC']]])
            ->get();

        // Query for deCheck
        $query = Transaction::join('merchants as m', 'm.merchant_id', '=', 'transactions.merchant_id')
            ->join('checking_accounts as ca', 'ca.transaction_id', '=', 'transactions.transaction_id')
            ->select('ca.account_number', DB::raw('COUNT(*) AS count'))
            ->where([
                ['transactions.status', '=', 'DECLINED'],
                ['transactions.date', '>=', $this->startTime],
                ['transactions.date', '<=', $this->endTime],
                ['transactions.test_mode', '=', 'L']
            ]);
            

        if (!empty($testChecks)) {
            $query = $query->whereNotIn('ca.account_number', $testChecks);
        }

        if (!empty($merchantExceptions)) {
            $query = $query->whereNotIn('m.merchant_name', $merchantExceptions);
        }

        $deCheck = $query->groupBy('ca.account_number')
            ->havingRaw("COUNT(*) > ?", [$thresholds[$constants['MDC']]])
            ->get();

        // Query for ddeCard
        $query = Transaction::join('merchants as m', 'm.merchant_id', '=', 'transactions.merchant_id')
            ->join('rooms as r', 'r.room_id', '=', 'transactions.room_id')
            ->join('creditcards as cc', 'cc.cc_transaction_id', '=', 'transactions.transaction_id')
            ->select('cc.cc_number', DB::raw('COUNT(*) AS count'), 'transactions.date')
            ->where([
                ['transactions.status', '=', 'DECLINED'],
                ['transactions.date', '>=', $this->startTime],
                ['transactions.date', '<=', $this->endTime],
                ['transactions.test_mode', '=', 'L'],
                ['r.three_d', '=', 0]
            ]);
            
    
        if (!empty($testCards)) {
            $query = $query->whereNotIn('cc.cc_number', $testCards);
        }
        
        if (!empty($merchantExceptions)) {
            $query = $query->whereNotIn('m.merchant_name', $merchantExceptions);
        }
        
        $ddeCard = $query->groupBy('cc.cc_number', DB::raw('date(transactions.date)'))
            ->havingRaw("COUNT(*) > ?", [$thresholds[$constants['DDC']]])
            ->get();
    
        // Query for ddeCheck
        $query = Transaction::join('merchants as m', 'm.merchant_id', '=', 'transactions.merchant_id')
            ->join('checking_accounts as ca', 'ca.transaction_id', '=', 'transactions.transaction_id')
            ->select('ca.account_number', DB::raw('COUNT(*) AS count'), 'transactions.date')
            ->where([
                ['transactions.status', '=', 'DECLINED'],
                ['transactions.date', '>=', $this->startTime],
                ['transactions.date', '<=', $this->endTime],
                ['transactions.test_mode', '=', 'L']
            ]);

        if (!empty($testChecks)) {
            $query = $query->whereNotIn('ca.account_number', $testChecks);
        }

        if (!empty($merchantExceptions)) {
            $query = $query->whereNotIn('m.merchant_name', $merchantExceptions);
        }

        $ddeCheck = $query->groupBy('ca.account_number', DB::raw('date(transactions.date)'))
            ->havingRaw("COUNT(*) > ?", [$thresholds[$constants['DDC']]])
            ->get();

        // Query for apACard
        $query = Transaction::join('merchants as m', 'm.merchant_id', '=', 'transactions.merchant_id')
            ->join('rooms as r', 'r.room_id', '=', 'transactions.room_id')
            ->join('creditcards as cc', 'cc.cc_transaction_id', '=', 'transactions.transaction_id')
            ->select('cc.cc_number', DB::raw('SUM(transactions.amount) AS amount'), 'transactions.date')
            ->where([
                ['transactions.status', '=', 'APPROVED'],
                ['transactions.date', '>=', $this->startTime],
                ['transactions.date', '<=', $this->endTime],
                ['transactions.test_mode', '=', 'L'],
                ['transactions.currency', '<>', 'CNY'],
                ['r.three_d', '=', 0]
            ]);

        if (!empty($testCards)) {
            $query = $query->whereNotIn('cc.cc_number', $testCards);
        }

        if (!empty($merchantExceptions)) {
            $query = $query->whereNotIn('m.merchant_name', $merchantExceptions);
        }

        $apACard = $query->groupBy('cc.cc_number', DB::raw('date(transactions.date)'))
            ->havingRaw("SUM(transactions.amount) > ?", [$thresholds[$constants['DAV']]])
            ->get();

        // Query for apACheck
        $query = Transaction::join('merchants as m', 'm.merchant_id', '=', 'transactions.merchant_id')
            ->join('checking_accounts as ca', 'ca.transaction_id', '=', 'transactions.transaction_id')
            ->select('ca.account_number', DB::raw('SUM(transactions.amount) AS amount'), 'transactions.date')
            ->where([
                ['transactions.status', '=', 'APPROVED'],
                ['transactions.date', '>=', $this->startTime],
                ['transactions.date', '<=', $this->endTime],
                ['transactions.test_mode', '=', 'L']
            ]);
        
        if (!empty($testChecks)) {
            $query = $query->whereNotIn('ca.account_number', $testChecks);
        }
        
        if (!empty($merchantExceptions)) {
            $query = $query->whereNotIn('m.merchant_name', $merchantExceptions);
        }
        
        $apACheck = $query->groupBy('ca.account_number', DB::raw('date(transactions.date)'))
            ->havingRaw("SUM(transactions.amount) > ?", [$thresholds[$constants['DAV']]])
            ->get();

        // Query for apCCard
        $query = Transaction::join('merchants as m', 'm.merchant_id', '=', 'transactions.merchant_id')
            ->join('rooms as r', 'r.room_id', '=', 'transactions.room_id')
            ->join('creditcards as cc', 'cc.cc_transaction_id', '=', 'transactions.transaction_id')
            ->select('cc.cc_number', DB::raw('COUNT(*) AS count'), 'transactions.date')
            ->where([
                ['transactions.status', '=', 'APPROVED'],
                ['transactions.date', '>=', $this->startTime],
                ['transactions.date', '<=', $this->endTime],
                ['transactions.test_mode', '=', 'L'],
                ['transactions.currency', '<>', 'CNY'],
                ['r.three_d', '=', 0]
            ]);
            
        
        if (!empty($testCards)) {
            $query = $query->whereNotIn('cc.cc_number', $testCards);
        }
        
        if (!empty($merchantExceptions)) {
            $query = $query->whereNotIn('m.merchant_name', $merchantExceptions);
        }
        
        $apCCard = $query->groupBy('cc.cc_number', DB::raw('date(transactions.date)'))
            ->havingRaw("COUNT(*) > ?", [$thresholds[$constants['DAC']]])
            ->get();

        // Query for apCCheck
        $query = Transaction::join('merchants as m', 'm.merchant_id', '=', 'transactions.merchant_id')
            ->join('checking_accounts as ca', 'ca.transaction_id', '=', 'transactions.transaction_id')
            ->select('ca.account_number', DB::raw('COUNT(*) AS count'), 'transactions.date')
            ->where([
                ['transactions.status', '=', 'APPROVED'],
                ['transactions.date', '>=', $this->startTime],
                ['transactions.date', '<=', $this->endTime],
                ['transactions.test_mode', '=', 'L']
            ]);
            

        if (!empty($merchantExceptions)) {
            $query = $query->whereNotIn('m.merchant_name', $merchantExceptions);
        }

        if (!empty($testChecks)) {
            $query = $query->whereNotIn('ca.account_number', $testChecks);
        }

        $apCCheck = $query->groupBy('ca.account_number', DB::raw('date(transactions.date)'))
            ->havingRaw("COUNT(*) > ?", [$thresholds[$constants['DAC']]])
            ->get();

        // Query for ipCard
        $query = Transaction::join('merchants as m', 'm.merchant_id', '=', 'transactions.merchant_id')
            ->join('customers as c', 'c.customer_id', '=', 'transactions.customer_id')
            ->join('rooms as r', 'r.room_id', '=', 'transactions.room_id')
            ->join('creditcards as cc', 'cc.cc_transaction_id', '=', 'transactions.transaction_id')
            ->select('cc.cc_number', DB::raw('COUNT(DISTINCT c.customer_ip) as count'))            
            ->where([
                ['transactions.date', '>=', $this->startTime],
                ['transactions.date', '<=', $this->endTime],
                ['transactions.test_mode', '=', 'L'],
                ['transactions.currency', '<>', 'CNY'],
                ['r.three_d', '=', 0]
            ])
            ->whereNotNull('c.customer_ip');

        if (!empty($approvedIps)) {
            $query = $query->whereNotIn('c.customer_ip', $approvedIps);
        }

        if (!empty($testCards)) {
            $query = $query->whereNotIn('cc.cc_number', $testCards);
        }

        if (!empty($merchantExceptions)) {
            $query = $query->whereNotIn('m.merchant_name', $merchantExceptions);
        }

        $ipCard = $query->groupBy('cc.cc_number')
            ->havingRaw('COUNT(DISTINCT c.customer_ip) > ?', [$thresholds[$constants['RCI']]])
            ->get();
        
        // Query for ipCheck
        $query = Transaction::join('merchants as m', 'm.merchant_id', '=', 'transactions.merchant_id')
            ->join('customers as c', 'c.customer_id', '=', 'transactions.customer_id')
            ->join('checking_accounts as ca', 'ca.transaction_id', '=', 'transactions.transaction_id')
            ->select('ca.account_number', DB::raw('COUNT(DISTINCT c.customer_ip) as count'))
            ->where([
                ['transactions.date', '>=', $this->startTime],
                ['transactions.date', '<=', $this->endTime],
                ['transactions.test_mode', '=', 'L']
            ])
            ->whereNotNull('c.customer_ip');
        
        if (!empty($approvedIps)) {
            $query = $query->whereNotIn('c.customer_ip', $approvedIps);
        }
               
        if (!empty($merchantExceptions)) {
            $query = $query->whereNotIn('m.merchant_name', $merchantExceptions);
        }
        
        $ipCheck = $query->groupBy('ca.account_number')
            ->havingRaw('COUNT(DISTINCT c.customer_ip) > ?', [$thresholds[$constants['RCI']]])
            ->get();
            
        // Query for ipReview
        $query = Transaction::join('merchants as m', 'm.merchant_id', '=', 'transactions.merchant_id')
            ->join('customers as c', 'c.customer_id', '=', 'transactions.customer_id')
            ->join('rooms as r', 'r.room_id', '=', 'transactions.room_id')
            ->join('creditcards as cc', 'cc.cc_transaction_id', '=', 'transactions.transaction_id')
            ->select('c.customer_ip', DB::raw('COUNT(*) as count'), DB::raw('COUNT(DISTINCT cc.cc_number) AS different_cards'))
            ->where([
                ['transactions.date', '>=', $this->startTime],
                ['transactions.date', '<=', $this->endTime],
                ['transactions.test_mode', '=', 'L'],
                ['transactions.currency', '<>', 'CNY'],
                ['r.three_d', '=', 0]
            ])
            ->whereNotNull('c.customer_ip');;
        
        if (!empty($approvedIps)) {
            $query = $query->whereNotIn('c.customer_ip', $approvedIps);
        }
        
        if (!empty($testCards)) {
            $query = $query->whereNotIn('cc.cc_number', $testCards);
        }
        
        if (!empty($merchantExceptions)) {
            $query = $query->whereNotIn('m.merchant_name', $merchantExceptions);
        }
        
        $ipReview = $query->groupBy('c.customer_ip')
            ->havingRaw('COUNT(*) > ?', [$thresholds[$constants['RIP']]])
            ->get();
            
        // Query for cvCard
        $cvCard = Transaction::on('whitelabel')
        ->join('merchants as m', 'm.merchant_id', '=', 'transactions.merchant_id')
        ->join('customers as c', 'c.customer_id', '=', 'transactions.customer_id')
        ->join('rooms as r', 'r.room_id', '=', 'transactions.room_id')
        ->join('creditcards as cc', 'cc.cc_transaction_id', '=', 'transactions.transaction_id')
        ->select(
            'cc.cc_number',
            DB::raw('COUNT(*) AS count'),
            DB::raw('COUNT(DISTINCT c.first_name) as different_first_names'),
            DB::raw('COUNT(DISTINCT c.last_name) as different_last_names'),
            DB::raw('COUNT(DISTINCT c.emailaddr) as different_emails'),
            DB::raw('COUNT(DISTINCT c.address1) as different_addresses')
        )
        ->where([
            ['transactions.date', '>=', $this->startTime],
            ['transactions.date', '<=', $this->endTime],
            ['transactions.test_mode', '=', 'L'],
            ['transactions.currency', '<>', 'CNY'],
            ['r.three_d', '=', 0]
        ])
        ->when(!empty($testCards), function($query) use ($testCards) {
            return $query->whereNotIn('cc.cc_number', $testCards);
        })
        ->when(!empty($merchantExceptions), function($query) use ($merchantExceptions) {
            return $query->whereNotIn('m.merchant_name', $merchantExceptions);
        })
        ->groupBy('cc.cc_number')
        ->havingRaw('(
                COUNT(DISTINCT c.first_name) > 1
                AND COUNT(DISTINCT c.last_name) > 1
                AND COUNT(DISTINCT c.emailaddr) > 1
                AND COUNT(DISTINCT c.address1) > 1
            ) OR (
                COUNT(DISTINCT c.first_name) > ?
                OR COUNT(DISTINCT c.last_name) > ?
                OR COUNT(DISTINCT c.emailaddr) > ?
                OR COUNT(DISTINCT c.address1) > ?
            )', [
                array_fill(0, 4,$thresholds[$constants['RCU']]),
        ])
        ->get();
        // Query for cvCheck
        $cvCheck = Transaction::on('whitelabel')
            ->join('merchants as m', 'm.merchant_id', '=', 'transactions.merchant_id')
            ->join('customers as c', 'c.customer_id', '=', 'transactions.customer_id')
            ->join('checking_accounts as ca', 'ca.transaction_id', '=', 'transactions.transaction_id')
            ->select(
                'ca.account_number',
                DB::raw('COUNT(*) AS count'),
                DB::raw('COUNT(DISTINCT c.first_name) as different_first_names'),
                DB::raw('COUNT(DISTINCT c.last_name) as different_last_names'),
                DB::raw('COUNT(DISTINCT c.emailaddr) as different_emails'),
                DB::raw('COUNT(DISTINCT c.address1) as different_addresses')
            )
            ->where([
                ['transactions.currency', '<>', 'CNY'],
                ['transactions.date', '>=', $this->startTime],
                ['transactions.date', '<=', $this->endTime],
                ['transactions.test_mode', '=', 'L']
            ])
            ->groupBy('ca.account_number')
            ->havingRaw('(
            COUNT(DISTINCT c.first_name) > 1
            AND COUNT(DISTINCT c.last_name) > 1
            AND COUNT(DISTINCT c.emailaddr) > 1
            AND COUNT(DISTINCT c.address1) > 1
        ) OR (
            COUNT(DISTINCT c.first_name) > ?
            OR COUNT(DISTINCT c.last_name) > ?
            OR COUNT(DISTINCT c.emailaddr) > ?
            OR COUNT(DISTINCT c.address1) > ?
        )', array_fill(0, 4, $thresholds[$constants['RCU']]))
        ->when(!empty($testChecks), function ($query) use ($testChecks) {
            return $query->whereNotIn('ca.account_number', $testChecks);
        })
        ->when(!empty($merchantExceptions), function ($query) use ($merchantExceptions) {
            return $query->whereNotIn('m.merchant_name', $merchantExceptions);
        })
        ->get();
    }

    protected function handleRiskTables($results, $check, $type = 'card', $field = 'count', $daily = false)
    {
        $this->existingRisk = []; // Initialize or retrieve accordingly.
        $this->newRisk = []; // Initialize or retrieve accordingly.

        $riskField = $this->getRiskField($type);

        foreach ($results as $qItem) {
            $hasExistingRisk = false;
            $hasExistingRiskDetail = false;

            foreach ($this->existingRisk as $riskItem) {
                $detailExists = false;
                if ($riskItem['risk_item'] == $qItem[$riskField]) { 
                    $hasExistingRisk = true;

                    if($riskItem['param'] == $check) {
                        $hasExistingRiskDetail = true;
                        $detailExists = ($riskItem['value'] == $qItem[$field]);
                    }

                    $infraction_date = empty($qItem['date']) ? now() : $qItem['date'];

                    if (!$detailExists) {
                        if (!$hasExistingRiskDetail) {
                            RiskyTransactionDetail::create([
                                'risky_transaction_id' => $riskItem['risky_transaction_id'],
                                'param' => $check,
                                'value' => $qItem[$field],
                                'infraction_date' => $infraction_date
                            ]);
                        } else {
                            RiskyTransactionDetail::where('risky_transaction_id', $riskItem['risky_transaction_id'])
                                ->update([
                                    'param' => $check,
                                    'value' => $qItem[$field],
                                    'infraction_date' => $infraction_date
                                ]);
                        }
                    }
                }
            }

            if (!$hasExistingRisk) {
                if (!empty($this->newRisk[$qItem[$riskField]])) {
                    $riskID = $this->newRisk[$qItem[$riskField]];
                } else {
                    $riskTransaction = RiskyTransaction::create([
                        'risk_item' => $qItem[$riskField],
                        'risk_type' => strtoupper($type),
                        'date_recorded' => now()
                    ]);
                    $riskID = $riskTransaction->id;

                    switch (strtoupper($type)) {
                        case "IP":
                            Customer::where('customer_ip', $qItem[$riskField])->update(['risk_id' => $riskID]);
                            break;
                        case "CARD":
                            CreditCard::where('cc_number', $qItem[$riskField])->update(['risk_id' => $riskID]);
                            break;
                        case "CHECK":
                            CheckingAccount::where('account_number', $qItem[$riskField])->update(['risk_id' => $riskID]);
                            break;
                    }
                }

                $infraction_date = empty($qItem['date']) ? now() : $qItem['date'];

                RiskyTransactionDetail::create([
                    'risky_transaction_id' => $riskID,
                    'param' => $check,
                    'value' => $qItem[$field],
                    'infraction_date' => $infraction_date
                ]);

                $this->newRisk[$qItem[$riskField]] = $riskID;
            }
        }
    }

    protected function getRiskField($type)
    {
        switch (strtoupper($type)) {
            case 'IP':
                return 'customer_ip';
            case 'EMAIL':
                return 'emailaddr';
            case 'CHECK':
                return 'account_number';
            default:
                return 'cc_number';
        }
    }
}