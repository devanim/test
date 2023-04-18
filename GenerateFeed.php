<?php

namespace HappyWebRo\Feeds\Console\Commands;

use HappyWebRo\Cars\Models\CarModelType;
use HappyWebRo\Feeds\Models\FeedData;
use HappyWebRo\PriceManagement\Services\Libraries\Price;
use Illuminate\Console\Command;
use PartDocuments;
use Storage;
use TDAPI;

class GeneratePieseAutoFeedRefactored extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'generate:piese-auto-feed-refactored';
    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Generate piese auto feed refactored';

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle()
    {
        Storage::disk('public')->put('files/feeds/pieseAuto/feed.csv_WIP_DO_NOT_DELETE', '"ID Produs";"Denumire Produs";"Categorii";"Descriere produs";"Moneda";"Pret produs";"Cantitate";"Poza"' . "\n");

        $this->generateFeedData();
        if (Storage::disk('public')->exists('files/feeds/pieseAuto/feed.csv_previous')) {
            Storage::disk('public')->delete('files/feeds/pieseAuto/feed.csv_previous');
        }
        if (Storage::disk('public')->exists('files/feeds/pieseAuto/feed.csv')) {
            Storage::disk('public')->move('files/feeds/pieseAuto/feed.csv', 'files/feeds/pieseAuto/feed.csv_previous');
        }
        if (Storage::disk('public')->exists('files/feeds/pieseAuto/feed.csv_WIP_DO_NOT_DELETE')) {
            Storage::disk('public')->move('files/feeds/pieseAuto/feed.csv_WIP_DO_NOT_DELETE', 'files/feeds/pieseAuto/feed.csv');
        }
        //$this->updatePieseAutoTitle();
    }

    public function generateFeedData()
    {
        $this->createFile();
        $products = FeedData::leftJoin('DenumiriProdusePieseAuto', function ($join) {
            $join->on('feed_data.id', '=', 'DenumiriProdusePieseAuto.id');
        })
            ->leftJoin('car_models', function ($join) {
                $join->on('feed_data.model_id', '=', 'car_models.id');
            })
            ->leftJoin('cars', function ($join) {
                $join->on('cars.id', '=', 'car_models.car_id');
            })
            ->leftJoin('car_model_groups', function ($join) {
                $join->on('car_model_groups.id', '=', 'car_models.model_group_id');
            })
            ->select(
                'feed_data.image as image',
                'feed_data.id as feed_id',
                'feed_data.model_id as model_id',
                'feed_data.product_id',
                'feed_data.pieseauto_title',
                'feed_data.categories as categorii',
                'feed_data.simple_title',
                'feed_data.title',
                'feed_data.product_id as product_id',
                'feed_data.brand as brand',
                'feed_data.code as code',
                'car_models.construction_start_year AS construction_start_year',
                'car_models.construction_end_year AS construction_end_year',
                'car_models.meta_title',
                'car_models.title as model_car',
                'cars.title as car',
                'cars.title as marca'
            )
            ->whereNotNull('feed_data.model_id')
            ->whereNull('feed_data.catalog')
            ->where('car_models.construction_start_year', '>=', 2000)
            ->where('feed_data.product_id', '=', 1990)
            ->where('feed_data.image', 'like', '%webservice%')
            ->orderBy('feed_data.product_id')
            ->chunk(5000, function ($chunk) {
                $i = $j = 0;
                $eligible_cars = ['ALFA ROMEO', 'AUDI', 'BMW', 'CHEVROLET', 'CHRYSLER', 'CITROEN', 'DACIA', 'DAEWOO', 'DODGE', 'FIAT', 'FORD', 'HONDA', 'HYUNDAI', 'ISUZU', 'IVECO', 'JAGUAR', 'JEEP', 'KIA', 'LANCIA', 'LAND ROVER', 'LEXUS', 'MAZDA', 'MERCEDES', 'MERCEDES BENZ', 'MERCEDES-BENZ', 'MINI', 'MITSUBISHI', 'NISSAN', 'OPEL', 'PEUGEOT', 'PORSCHE', 'RENAULT', 'SAAB', 'SEAT', 'SKODA', 'SMART', 'SSANGYONG', 'SUBARU', 'SUZUKI', 'TOYOTA', 'VOLKSWAGEN', 'VOLVO', 'VW'];
                $items = [
                    'id_produs' => 'null',
                    'denumire_produs' => 'null',
                    'categorii' => 'null',
                    'descriere_produs' => 'null',
                    'moneda' => 'RON',
                    'pret_produs' => 0,
                    'cantitate' => 0,
                    'categorii' => 'null',
                    'poza' => 'null',
                ];
                $previous_product_id = 0;
                $oenNumberUL = '';
                foreach ($chunk as $key => $line) {
                    $title = '';
                    if (isset($line->construction_start_year) && !empty($line->construction_start_year) && $line->construction_start_year >= 2000 && in_array(strtoupper($line->car), $eligible_cars)) {
                        $construction_end_year = ($line->construction_end_year == 0) ? 'prezent' : $line->construction_end_year;
                        $max_construction_end_year = ($line->construction_end_year == 0) ? date('Y') : $line->construction_end_year;

                        if ($line->simple_title != '') {
                            $title = ((isset($line->pieseauto_title) && !empty($line->pieseauto_title) && trim($line->pieseauto_title) != '') ? $line->pieseauto_title : $line->simple_title) . ' ' . $line->meta_title . ' ' . implode(' ', range($line->construction_start_year, $max_construction_end_year)) . ' ' . $line->brand . ' ' . $line->code;
                        } else {
                            $title = ((isset($line->pieseauto_title) && !empty($line->pieseauto_title) && trim($line->pieseauto_title) != '') ? $line->pieseauto_title : $line->title) . ' ' . $line->meta_title . ' ' . implode(' ', range($line->construction_start_year, $max_construction_end_year)) . ' ' . $line->brand . ' ' . $line->code;
                        }
                    }

                    $title = preg_replace("/[^A-Za-z0-9_\-.ăĂâÂțȚîÎșȘ; ,]/", ' ', $title);

                    $priceInfo = new Price();
                    $price = $priceInfo->quickFinalPrice($line->product_id, $line->brand, $line->code);
                    if ($price != null && $title && !empty($title) && $price >= 30) { //30
                        if ($previous_product_id != $line->product_id) {
                            $image_link = asset('public/photos/nopic/no-product.jpg');

                            $line_image_link = $line->image;

                            if (str_contains($line->image, 'webservice.tecalliance.services/pegasus-3-0')) {
                                $line_image_link = substr($line->image, 0, -2);
                            }

                            $car_model_types = TDAPI::productCarModelTypes($line->product_id);
                            ++$i;
                            $oenNumbers = TDAPI::productListWithDetailsAndOE($line->product_id);
                            if (isset($oenNumbers->data)) {
                                if (isset($oenNumbers->data->array)) {
                                    if (isset($oenNumbers->data->array[0]->oenNumbers)) {
                                        if (isset($oenNumbers->data->array[0]->oenNumbers->array)) {
                                            $oenNumberUL = '<li><b>Coduri compatibile:</b><ul>';
                                            foreach ($oenNumbers->data->array[0]->oenNumbers->array as $oenNumber) {
                                                $oenNumberUL .= '<li>' . $oenNumber->oeNumber . ' - ' . $oenNumber->brandName . '</li>';
                                            }
                                            $oenNumberUL .= '</ul></li>';
                                        }
                                    }
                                    if (isset($oenNumbers->data->array[0]->articleDocuments->array)) {
                                        $documents = collect($oenNumbers->data->array[0]->articleDocuments->array);
                                        $image = PartDocuments::firstImage($documents);
                                        if (isset($image->docId)) {
                                            $image_link = preg_replace('/\s+/', '', 'https://webservice.tecalliance.services/pegasus-3-0/documents/' . config('tecdoc.td_provider') . '/' . $image->docId);
                                            if (strcmp($line_image_link, $image_link) !== 0) {
                                                $line_image_link = $image_link;
                                            }
                                        }
                                    }
                                }
                            }
                            $previous_product_id = $line->product_id;
                        }
                        $matched_engine_codes = false;

                        if (!$car_model_types->isEmpty()) {
                            $style = "style='border-collapse: collapse'";
                            $style_td = $style_th = "style='border: 1px solid #dddddd'";
                            $engine_codes =
                                '<table ' . $style . '>' .
                                '<tr>' .
                                '<th ' . $style_th . '>Cod motor</th>' .
                                '<th ' . $style_th . '>An constructie (de la)</th>' .
                                '<th ' . $style_th . '>An constructie (pana la)</th>' .
                                '<th ' . $style_th . '>Putere</th>' .
                                '<th ' . $style_th . '>Capacitate cilindrica [cm³]</th>' .
                                '<th ' . $style_th . '>Caroserie</th>' .
                                '<th ' . $style_th . '>Tip combustibil</th>' .
                                '<th ' . $style_th . '>Tractiune</th>' .
                                '</tr>';
                            foreach ($car_model_types as $car_model_type) {
                                if ($car_model_type->model_id == $line->model_id) {
                                    $matched_engine_codes = true;
                                    $engine_codes .=
                                        '<tr>' .
                                        '<td ' . $style_td . '>' . $car_model_type->engine_codes . '</td>' .
                                        '<td ' . $style_td . '>' . $car_model_type->construction_start_year . '</td>' .
                                        '<td ' . $style_td . '>' . (($car_model_type->construction_end_year == 0) ? 'prezent' : $car_model_type->construction_end_year) . '</td>' .
                                        '<td ' . $style_td . '>' . $car_model_type->hp . ' hp (' . $car_model_type->kw . ' kw)</td>' .
                                        '<td ' . $style_td . '>' . $car_model_type->cc . '</td>' .
                                        '<td ' . $style_td . '>' . $car_model_type->body . '</td>' .
                                        '<td ' . $style_td . '>' . $car_model_type->fuel . '</td>' .
                                        '<td ' . $style_td . '>' . $car_model_type->axle . '</td>' .
                                        '</tr>';
                                }
                            }
                            $engine_codes .= '</table>';
                        }
                        // this is to remove the table header if there is no compatible
                        // engine codes returned
                        if (!$matched_engine_codes) {
                            $engine_codes = '';
                        }

                        $construction_end_year = ($line->construction_end_year == 0) ? 'prezent' : $line->construction_end_year;

                        $extra_description =
                            '<ul>' .
                            '<li><b>Marca: </b>' . $this->getCarBrandFormatted($line) . '</li>' .
                            '<li><b>Model: </b>' . $line->titlu_tip_model . ' (' . $line->construction_start_year . '-' . $construction_end_year . ')</li>' .
                            '<li><b>An constructie (de la): </b>' . $line->construction_start_year . '</li>' .
                            '<li><b>An constructie (pana la): </b>' . $construction_end_year . '</li>' .
                            '<li><b>Brand Piesa: </b>' . $line->brand . '</li>' .
                            '<li><b>Cod Produs: </b>' . preg_replace('/\s+/', '', $line->code) . '</li>' .
                            $oenNumberUL .
                            '</ul>';

                        $categories = explode('|', $line->categorii, -1);
                        $category = end($categories);

                        $items['id_produs'] = md5($line->product_id . $line->model_id);
                        $items['denumire_produs'] = $title;
                        $items['categorii'] = (mb_strlen($category) > 0) ? $category : 'Alte piese';
                        $items['descriere_produs'] = implode(' ', ['<h3>', $title, '</h3>', $line->description, $extra_description, $engine_codes]);
                        $items['pret_produs'] = $price;
                        $items['cantitate'] = 50;
                        $items['poza'] = trim($line_image_link);

                        $csv_line = implode(
                            '; ',
                            array_map(
                                function ($v, $k) {
                                        return $v;
                                    },
                                $items,
                                array_keys($items)
                            )
                        );

                        $disk = Storage::disk('public');
                        $file = fopen($disk->path('files/feeds/pieseAuto/feed.csv_WIP_DO_NOT_DELETE'), 'a+');
                        ++$j;
                        fwrite($file, $csv_line . "\n");
                        fclose($file);
                    }
                }
                echo '[INFO] :: ' . date('Y-m-d\TH:i:sP', time()) . ' :: Finished reading ' . count($chunk) . ' rows' . "\n";
                echo '[INFO] :: ' . date('Y-m-d\TH:i:sP', time()) . ' :: Finished writing ' . $j . ' rows' . "\n";
                echo '[INFO] :: ' . date('Y-m-d\TH:i:sP', time()) . ' :: Called productListWithDetailsAndOE() ' . $i . ' times' . "\n";
            });
        $this->gzip('files/feeds/pieseAuto/feed.csv');
    }

    private function createFile()
    {
        Storage::disk('public')->delete('files/feeds/pieseAuto/feed.csv');
        Storage::disk('public')->put('files/feeds/pieseAuto/feed.csv', '');

        return Storage::disk('public')->path('files/feeds/pieseAuto/feed.csv');
    }

    public function gzip($filename, $disk = 'public', $delete_original = false)
    {
        $disk = Storage::disk($disk);
        $data = $disk->get($filename);
        $out_file = "$filename.gz";

        $gzdata = gzencode($data, 9);
        $disk->put($out_file, $gzdata);
        $fp = fopen($disk->path($out_file), 'w');
        $result = fwrite($fp, $gzdata);
        fclose($fp);

        if ($result && $delete_original) {
            $disk->delete($filename);
        }

        return $result > 0;
    }

    public function getCarBrandFormatted($row)
    {
        $car_brand = '';
        switch ($row->marca) {
            case preg_match('/bmw/i', $row->marca) ? true : false:
                $car_brand = strtoupper($row->marca);
                break;
            case preg_match('/mercedes/i', $row->marca) ? true : false:
                $car_brand = 'Mercedes-Benz';
                break;
            case preg_match('/citr/i', $row->marca) ? true : false:
                $car_brand = 'Citroen';
                break;
            case preg_match('/alfa/i', $row->marca) ? true : false:
                $car_brand = 'Alfa Romeo';
                break;
            case preg_match('/vw/i', $row->marca) ? true : false:
                $car_brand = 'Volkswagen';
                break;
            default:
                $car_brand = ucfirst(strtolower($row->marca));
        }

        return $car_brand;
    }

    public function getCarModelType($model_id)
    {
        return $products = CarModelType::whereModelId($model_id)->where('car_model_types.construction_start_year', '>=', 2000)->select('car_model_types.*')->get();
    }
}